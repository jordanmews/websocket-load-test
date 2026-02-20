package main

import (
	"gows-man/helpers"

	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

func parseCLI() (domain *string, port *string) {
	port = flag.String("port", "8080", "Port to listen on")
	domain = flag.String("domain", "localhost", "Domain")
	flag.Parse()
	return
}

var stats = &helpers.Stats{}

// Converts http connections to ws connections
// TODO: Make CheckOrigin strict for prod use. Currently, returns true to allow connections from any origin only for testing
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// A handler for each websocket connection to the server.
	// Holds methods for:
	// - ping/pong timeouts
	// - handle explicit errors from the client
	// - send periodic updates to the client

	// Upgrade http conn to ws conn
	// standard ws handshake process
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		stats.IncrementErrors()
		log.Printf("Upgrade error: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("Connection established.")
	stats.IncrementActive()
	defer stats.DecrementActive()

	// Pong event handler
	// After pong is received, reset the next read deadline to 60 seconds in the future
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Set the read deadline to 60 seconds in future to kick off the function
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	// Send connection confirmed message
	confirmMsg := helpers.NewInfoMsg("Connection establsished.")

	if err := conn.WriteJSON(confirmMsg); err != nil {
		stats.IncrementErrors()
		log.Printf("Write error: %v", err)
		return
	}

	//region Define Channels

	// Periodic message channel
	msgTicker := time.NewTicker(1 * time.Second)
	defer msgTicker.Stop()

	// Ping channel
	pingTicker := time.NewTicker(1 * time.Second)
	defer pingTicker.Stop()

	activeConnectionMonitor := time.NewTicker(1 * time.Second)
	defer activeConnectionMonitor.Stop()

	// "Done" channel for when client disconnects
	done := make(chan struct{})

	//endregion

	// goroutine to read all messages from client and exit on any errors
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				if websocket.IsCloseError(
					err,
					websocket.CloseNormalClosure,
					websocket.CloseGoingAway) {
					log.Println("Client disconnected normally.")
				} else {
					stats.IncrementErrors()
					log.Printf("Read error: %v", err)
				}
				return
			}
		}
	}()

	messageCount := 0

	// Loop until done channel is closed
	for {
		select {
		case <-pingTicker.C:
			log.Println("Sending ping to client.")
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				stats.IncrementErrors()
				log.Printf("Ping error: %v", err)
				return
			}
		case <-msgTicker.C:
			messageCount++
			msg := helpers.NewHelloFeedMsg(fmt.Sprintf("hello #%d", messageCount))
			if err := conn.WriteJSON(msg); err != nil {
				stats.IncrementErrors()
				log.Printf("Sending message error: %v", err)
				return
			}
		case <-activeConnectionMonitor.C:
			fmt.Println("Active connections: ", stats.GetActive())
		case <-done:
			closeMsg := helpers.NewInfoMsg("Connection closing.")
			conn.WriteJSON(closeMsg)
			return
		}
	}

}

func main() {

	domain, port := parseCLI()

	http.HandleFunc("/ws", handleWebSocket)

	addr := *domain + ":" + *port
	log.Printf("Websocket server starting on %s", addr)

	// Start the HTTP server
	if err := http.ListenAndServe(addr, nil); err != nil {
		stats.IncrementErrors()
		log.Fatal(err)
	}
}
