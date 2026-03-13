package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"
)

// SignalMessage is the JSON envelope for all WebSocket messages.
type SignalMessage struct {
	Type         string          `json:"type"`
	RoomID       string          `json:"roomId,omitempty"`
	UserID       string          `json:"userId,omitempty"`
	FromID       string          `json:"fromId,omitempty"`
	SDP          string          `json:"sdp,omitempty"`
	Candidate    json.RawMessage `json:"candidate,omitempty"`
	Participants []string        `json:"participants,omitempty"`
}

// Participant represents a single user in a room.
type Participant struct {
	ID             string
	Conn           *websocket.Conn
	PeerConnection *webrtc.PeerConnection
	Room           *Room

	// Tracks this participant publishes (forwarded to others)
	OutputTracks map[string]*webrtc.TrackLocalStaticRTP
	// RTP senders for tracks from other participants added to this PC
	TrackSenders map[string][]*webrtc.RTPSender

	// pendingTracks are tracks to add before the next answer
	PendingTracks []pendingTrack

	mu sync.Mutex
}

type pendingTrack struct {
	sourceID string
	track    *webrtc.TrackLocalStaticRTP
}

// SendJSON sends a JSON message over the WebSocket (thread-safe).
func (p *Participant) SendJSON(msg SignalMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.Conn.WriteJSON(msg); err != nil {
		log.Printf("ws write error for %s: %v", p.ID, err)
	}
}

// RequestRenegotiation asks the client to create a new offer.
func (p *Participant) RequestRenegotiation() {
	p.SendJSON(SignalMessage{Type: "renegotiate"})
}

// AddPendingTracks adds all pending tracks to the PeerConnection.
// Must be called before creating an answer.
func (p *Participant) AddPendingTracks() {
	p.mu.Lock()
	pending := p.PendingTracks
	p.PendingTracks = nil
	p.mu.Unlock()

	for _, pt := range pending {
		sender, err := p.PeerConnection.AddTrack(pt.track)
		if err != nil {
			log.Printf("add pending track error for %s from %s: %v", p.ID, pt.sourceID, err)
			continue
		}
		p.mu.Lock()
		p.TrackSenders[pt.sourceID] = append(p.TrackSenders[pt.sourceID], sender)
		p.mu.Unlock()

		// Read and discard RTCP
		go func(s *webrtc.RTPSender) {
			buf := make([]byte, 1500)
			for {
				if _, _, err := s.Read(buf); err != nil {
					return
				}
			}
		}(sender)
	}
}

// AddTrackFrom queues a remote participant's output track and asks the client to renegotiate.
func (p *Participant) AddTrackFrom(sourceID string, track *webrtc.TrackLocalStaticRTP) {
	p.mu.Lock()
	p.PendingTracks = append(p.PendingTracks, pendingTrack{sourceID, track})
	p.mu.Unlock()

	p.RequestRenegotiation()
}

// RemoveTracksFrom removes all tracks originating from sourceID.
func (p *Participant) RemoveTracksFrom(sourceID string) {
	p.mu.Lock()
	senders := p.TrackSenders[sourceID]
	delete(p.TrackSenders, sourceID)
	p.mu.Unlock()

	for _, sender := range senders {
		if err := p.PeerConnection.RemoveTrack(sender); err != nil {
			log.Printf("remove track error: %v", err)
		}
	}
	if len(senders) > 0 {
		p.RequestRenegotiation()
	}
}

// Room represents a meeting room.
type Room struct {
	ID           string
	Participants map[string]*Participant
	mu           sync.RWMutex
}

// Broadcast sends a message to all participants except excludeID.
func (r *Room) Broadcast(msg SignalMessage, excludeID string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, p := range r.Participants {
		if id != excludeID {
			p.SendJSON(msg)
		}
	}
}

// AddParticipant adds a participant and wires up track forwarding.
func (r *Room) AddParticipant(p *Participant) {
	r.mu.Lock()

	// Collect existing participants and their tracks
	existingIDs := make([]string, 0, len(r.Participants))
	type trackInfo struct {
		sourceID string
		track    *webrtc.TrackLocalStaticRTP
	}
	var existingTracks []trackInfo

	for id, existing := range r.Participants {
		existingIDs = append(existingIDs, id)
		existing.mu.Lock()
		for _, t := range existing.OutputTracks {
			existingTracks = append(existingTracks, trackInfo{id, t})
		}
		existing.mu.Unlock()
	}

	r.Participants[p.ID] = p
	r.mu.Unlock()

	// Send room info to new participant
	p.SendJSON(SignalMessage{
		Type:         "room-info",
		UserID:       p.ID,
		Participants: existingIDs,
	})

	// Notify existing participants
	r.Broadcast(SignalMessage{
		Type:   "participant-joined",
		UserID: p.ID,
	}, p.ID)

	// Add existing tracks to new participant's PC (will trigger renegotiation)
	for _, ti := range existingTracks {
		p.AddTrackFrom(ti.sourceID, ti.track)
	}
}

// RemoveParticipant removes a participant and cleans up.
func (r *Room) RemoveParticipant(userID string) {
	r.mu.Lock()
	p, ok := r.Participants[userID]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.Participants, userID)
	remaining := make([]*Participant, 0, len(r.Participants))
	for _, other := range r.Participants {
		remaining = append(remaining, other)
	}
	r.mu.Unlock()

	// Close peer connection
	if p.PeerConnection != nil {
		p.PeerConnection.Close()
	}

	// Remove this participant's tracks from all others
	for _, other := range remaining {
		other.RemoveTracksFrom(userID)
	}

	// Broadcast leave
	r.Broadcast(SignalMessage{
		Type:   "participant-left",
		UserID: userID,
	}, "")
}

// RoomManager manages all rooms.
type RoomManager struct {
	Rooms map[string]*Room
	mu    sync.RWMutex
}

// NewRoomManager creates a new RoomManager.
func NewRoomManager() *RoomManager {
	return &RoomManager{Rooms: make(map[string]*Room)}
}

// GetOrCreateRoom returns an existing room or creates a new one.
func (rm *RoomManager) GetOrCreateRoom(roomID string) *Room {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if room, ok := rm.Rooms[roomID]; ok {
		return room
	}
	room := &Room{
		ID:           roomID,
		Participants: make(map[string]*Participant),
	}
	rm.Rooms[roomID] = room
	return room
}

// RemoveParticipant removes a participant and cleans up the room if empty.
func (rm *RoomManager) RemoveParticipant(roomID, userID string) {
	rm.mu.RLock()
	room, ok := rm.Rooms[roomID]
	rm.mu.RUnlock()
	if !ok {
		return
	}

	room.RemoveParticipant(userID)

	room.mu.RLock()
	empty := len(room.Participants) == 0
	room.mu.RUnlock()

	if empty {
		rm.mu.Lock()
		delete(rm.Rooms, roomID)
		rm.mu.Unlock()
		log.Printf("room %s deleted (empty)", roomID)
	}
}

// CreatePeerConnection creates a new PeerConnection with default codecs.
func CreatePeerConnection() (*webrtc.PeerConnection, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))

	return api.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	})
}

// SetupOnTrack configures the OnTrack handler for a participant's PeerConnection.
func SetupOnTrack(p *Participant) {
	p.PeerConnection.OnTrack(func(remoteTrack *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("track received from %s: %s (kind: %s)", p.ID, remoteTrack.ID(), remoteTrack.Kind())

		// Create a local track that mirrors this remote track
		localTrack, err := webrtc.NewTrackLocalStaticRTP(
			remoteTrack.Codec().RTPCodecCapability,
			remoteTrack.ID(),
			p.ID, // streamID = participant's user ID
		)
		if err != nil {
			log.Printf("new local track error: %v", err)
			return
		}

		p.mu.Lock()
		p.OutputTracks[remoteTrack.ID()] = localTrack
		p.mu.Unlock()

		// Add this track to all other participants
		p.Room.mu.RLock()
		for id, other := range p.Room.Participants {
			if id != p.ID {
				other.AddTrackFrom(p.ID, localTrack)
			}
		}
		p.Room.mu.RUnlock()

		// PLI ticker for video tracks
		if remoteTrack.Kind() == webrtc.RTPCodecTypeVideo {
			go func() {
				ticker := time.NewTicker(3 * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					if p.PeerConnection.ConnectionState() == webrtc.PeerConnectionStateClosed {
						return
					}
					if err := p.PeerConnection.WriteRTCP([]rtcp.Packet{
						&rtcp.PictureLossIndication{MediaSSRC: uint32(remoteTrack.SSRC())},
					}); err != nil {
						return
					}
				}
			}()
		}

		// RTP forwarding loop
		buf := make([]byte, 1500)
		for {
			n, _, readErr := remoteTrack.Read(buf)
			if readErr != nil {
				return
			}
			if _, writeErr := localTrack.Write(buf[:n]); writeErr != nil {
				return
			}
		}
	})
}
