package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var roomManager = NewRoomManager()

func main() {
	http.Handle("/", http.FileServer(http.Dir("web")))
	http.HandleFunc("/ws", handleWebSocket)

	log.Println("Server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade error: %v", err)
		return
	}
	defer conn.Close()

	var participant *Participant
	var roomID string

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var msg SignalMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("json parse error: %v", err)
			continue
		}

		switch msg.Type {
		case "join":
			if participant != nil {
				continue // already joined
			}
			roomID = msg.RoomID
			room := roomManager.GetOrCreateRoom(roomID)

			pc, pcErr := CreatePeerConnection()
			if pcErr != nil {
				log.Printf("pc creation error: %v", pcErr)
				continue
			}

			participant = &Participant{
				ID:             msg.UserID,
				Conn:           conn,
				PeerConnection: pc,
				Room:           room,
				OutputTracks:   make(map[string]*webrtc.TrackLocalStaticRTP),
				TrackSenders:   make(map[string][]*webrtc.RTPSender),
			}

			// ICE candidate trickle: server → client
			pc.OnICECandidate(func(c *webrtc.ICECandidate) {
				if c == nil {
					return
				}
				candidateJSON, _ := json.Marshal(c.ToJSON())
				participant.SendJSON(SignalMessage{
					Type:      "ice-candidate",
					Candidate: candidateJSON,
				})
			})

			pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
				log.Printf("participant %s connection state: %s", participant.ID, state.String())
			})

			SetupOnTrack(participant)
			room.AddParticipant(participant)
			log.Printf("participant %s joined room %s", msg.UserID, roomID)

		case "offer":
			if participant == nil {
				continue
			}
			// Add any pending tracks before answering
			participant.AddPendingTracks()

			sdp := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: msg.SDP}
			if err := participant.PeerConnection.SetRemoteDescription(sdp); err != nil {
				log.Printf("set remote desc (offer) error: %v", err)
				continue
			}
			answer, err := participant.PeerConnection.CreateAnswer(nil)
			if err != nil {
				log.Printf("create answer error: %v", err)
				continue
			}
			if err := participant.PeerConnection.SetLocalDescription(answer); err != nil {
				log.Printf("set local desc error: %v", err)
				continue
			}
			participant.SendJSON(SignalMessage{Type: "answer", SDP: answer.SDP})

		case "ice-candidate":
			if participant == nil || msg.Candidate == nil {
				continue
			}
			var candidate webrtc.ICECandidateInit
			if err := json.Unmarshal(msg.Candidate, &candidate); err != nil {
				log.Printf("ice candidate parse error: %v", err)
				continue
			}
			if err := participant.PeerConnection.AddICECandidate(candidate); err != nil {
				log.Printf("add ice candidate error: %v", err)
			}
		}
	}

	// Cleanup on disconnect
	if participant != nil {
		log.Printf("participant %s disconnected from room %s", participant.ID, roomID)
		roomManager.RemoveParticipant(roomID, participant.ID)
	}
}
