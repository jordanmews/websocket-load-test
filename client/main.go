package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"websocket-load-test/helpers"

	"github.com/gorilla/websocket"
)

type Config struct {
	ServerURL       string
	MaxConnections  int
	RampUpSeconds   int
	HoldSeconds     int
	RampDownSeconds int
	VerifyStrings   []string
	ErrorLogFile    string
}

func parseCLI() *Config {

	// CLI flags and defaults
	serverURL := flag.String("server", "ws://localhost:8080/ws", "WebSocket server URL")
	maxConnections := flag.Int("connections", 10, "Maximum concurrent connections")
	rampUpSeconds := flag.Int("rampup", 1, "Seconds to ramp up to max connections")
	holdSeconds := flag.Int("hold", 5, "Seconds to hold connections open")
	errorLogFile := flag.String("errorlog", "client-errors.csv", "Path to error log CSV file")
	// todo - Implement rampDownSeconds in future. Currently this just mirrors the rampUp as each connection is separately held and ramps down after holdSeconds
	//rampDownSeconds := flag.Int("rampdown", 0, "Seconds to ramp down to zero connections")

	flag.Parse()

	return &Config{
		ServerURL:      *serverURL,
		MaxConnections: *maxConnections,
		RampUpSeconds:  *rampUpSeconds,
		HoldSeconds:    *holdSeconds,
		ErrorLogFile:   *errorLogFile,
	}
}

func handleConnection(connID int, config *Config, stats *helpers.Stats, errorLogger *helpers.ErrorLogger, waitGroup *sync.WaitGroup) {

	defer waitGroup.Done()

	stats.IncrementActive()
	defer stats.DecrementActive()

	// Init websocket connection
	conn, _, err := websocket.DefaultDialer.Dial(config.ServerURL, nil)
	if err != nil {
		log.Printf("Connection %d: Dial error: %v", connID, err)
		stats.IncrementErrors()
		errorLogger.Log(helpers.ErrorRecord{
			ConnectionID: connID,
			Time:         time.Now().Format(time.RFC3339),
			Expected:     "successful connection",
			Actual:       fmt.Sprintf("dial error: %v", err),
		})
		return
	}
	defer conn.Close()

	// Calculate connection End Time for each connection using HoldSeconds
	var connEndTime = time.Now().Add(time.Duration(config.HoldSeconds) * time.Second)

	for {

		// If HoldSeconds is expired, close the connection and exit the loop
		if time.Now().After(connEndTime) {
			log.Printf("Hold duration of %d exceeded for connID: %d. Closing connection.", config.HoldSeconds, connID)

			err := conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client done"),
			)

			if err != nil {
				log.Println("Error sending close:", err)
			}
			time.Sleep(time.Second)
			err = conn.Close()
			if err != nil {
				log.Printf("Error in closing connection: %v", err)
			}
			return
		}

		// Check for normal closure messages from server
		var msg helpers.WsMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("Connection %d: Closed normally", connID)
			} else {
				log.Printf("Connection %d: Read error: %v", connID, err)
			}
			return
		}

		// Example of asserting message types are sending the correct content
		TestWsMessageTypeContainsMsg("hello", "hello #", msg)

		msgJSON, _ := json.Marshal(msg)
		log.Printf("Connection %d: Recieved: %s", connID, string(msgJSON))

	}
}

func rampConnections(config *Config, stats *helpers.Stats, errorLogger *helpers.ErrorLogger) {
	var wg sync.WaitGroup

	// The interval between starting each connection is calculated by dividing RampUpSeconds by MaxConnections
	rampUpInterval := time.Duration(0)
	if config.RampUpSeconds > 0 && config.MaxConnections > 0 {
		rampUpInterval = time.Duration(config.RampUpSeconds) * time.Second / time.Duration(config.MaxConnections)
	}

	log.Printf("Starting ramp-up: %d connections over %d seconds", config.MaxConnections, config.RampUpSeconds)

	for i := 1; i <= config.MaxConnections; i++ {
		wg.Add(1)

		go handleConnection(i, config, stats, errorLogger, &wg)

		if rampUpInterval > 0 && i < config.MaxConnections {
			time.Sleep(rampUpInterval)
		}

		if i%100 == 0 || i == config.MaxConnections {
			log.Printf("Ramp-up progress: %d/%d connections (Active: %d, Errors: %d)", i, config.MaxConnections, stats.GetActive(), stats.GetErrors())
		}
	}

	// This blocks until wg.Done() has been called for each wg.Add(1)
	// We handle the HoldSeconds for each connection inside of each handleconnection goroutine
	wg.Wait()

}

func main() {

	config := parseCLI()

	// init error logger
	errorLogger, err := helpers.NewErrorLogger(config.ErrorLogFile)
	if err != nil {
		log.Fatalf("Failed to create error logger: %v", err)
	}
	// test error logger functionality - leave commented out by default
	//errorLogger.Log(helpers.ErrorRecord{Expected: "test", Actual: "just-testing"})
	defer errorLogger.Close()

	stats := &helpers.Stats{}

	log.Printf("Starting websocket load test")
	log.Printf("Config: %+v", config)

	rampConnections(config, stats, errorLogger)

	log.Printf("Load test complete")
}

func TestWsMessageTypeContainsMsg(targetType string, expected string, msg helpers.WsMessage) bool {
	// If the message type equals targetType (i.e. hello), then check the msg for a string containing 'expected'

	// Example expected string
	//jsonString = `{"type":"hello","loglevel":"info","time":"2024-01-15T10:30:00Z","msg":"hello #1}`

	if msg.Type == targetType {
		log.Println(msg.Type)
		if strings.Contains(msg.Msg, expected) {
			return true
		} else {
			log.Printf("targetType: '%v', missing expected value in msg: '%v'", targetType, expected)
			return false
		}
	}
	return false
}
