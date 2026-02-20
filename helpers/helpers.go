package helpers

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// WsMessage is the json sent to client at intervals
type WsMessage struct {
	Type     string `json:"type"`
	Msg      string `json:"msg"`
	LogLevel string `json:"loglevel"`
	Time     string `json:"time"`
}

func NewInfoMsg(msg string) WsMessage {
	return WsMessage{
		Msg:      msg,
		LogLevel: "info",
		Time:     time.Now().Format(time.RFC3339),
	}
}

func NewHelloFeedMsg(msg string) WsMessage {
	return WsMessage{
		Type:     "hello",
		Msg:      msg,
		LogLevel: "info",
		Time:     time.Now().Format(time.RFC3339),
	}
}

func NewWarnMsg(msg string) WsMessage {
	return WsMessage{
		Msg:      msg,
		LogLevel: "warn",
		Time:     time.Now().Format(time.RFC3339),
	}
}

func NewErrMsg(msg string) WsMessage {
	return WsMessage{
		Msg:      msg,
		LogLevel: "error",
		Time:     time.Now().Format(time.RFC3339),
	}
}

func NewDebugMsg(msg string) WsMessage {
	return WsMessage{
		Msg:      msg,
		LogLevel: "debug",
		Time:     time.Now().Format(time.RFC3339),
	}
}

// Error record for CSV log
type ErrorRecord struct {
	ConnectionID int
	Time         string
	Expected     string
	Actual       string
}

// Use atomic package for safe concurrent access to these counters
type Stats struct {
	activeConnections int64
	totalErrors       int64
}

func (s *Stats) IncrementActive() {
	atomic.AddInt64(&s.activeConnections, 1)
}

func (s *Stats) DecrementActive() {
	atomic.AddInt64(&s.activeConnections, -1)
}

func (s *Stats) IncrementErrors() {
	atomic.AddInt64(&s.totalErrors, 1)
}

func (s *Stats) GetActive() int64 {
	return atomic.LoadInt64(&s.activeConnections)
}

func (s *Stats) GetErrors() int64 {
	return atomic.LoadInt64(&s.totalErrors)
}

// For threadsafe writing to errors CSV file
type ErrorLogger struct {
	mu     sync.Mutex
	writer *csv.Writer
	file   *os.File
}

func NewErrorLogger(filename string) (*ErrorLogger, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(file)

	stat, _ := file.Stat()
	if stat.Size() == 0 {
		writer.Write([]string{"ConnectionID", "Time", "Expected", "Actual"})
		writer.Flush()
	}

	return &ErrorLogger{
		writer: writer,
		file:   file,
	}, nil
}

func (el *ErrorLogger) Log(record ErrorRecord) {
	el.mu.Lock()
	defer el.mu.Unlock()

	el.writer.Write([]string{
		fmt.Sprintf("%d", record.ConnectionID),
		record.Time,
		record.Expected,
		record.Actual,
	})
	el.writer.Flush()
}

func (el *ErrorLogger) Close() {
	el.writer.Flush()
	el.file.Close()
}
