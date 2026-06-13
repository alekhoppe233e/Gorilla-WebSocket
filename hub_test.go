package main

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHubGoroutineLeak(t *testing.T) {
	// Override timeouts for faster test execution
	writeWait = 1 * time.Second
	pongWait = 2 * time.Second
	pingPeriod = (pongWait * 9) / 10

	// 1. Capture the baseline goroutine count
	runtime.GC()
	baseline := runtime.NumGoroutine()

	// Start the Hub
	hub := newHub()
	go hub.run()

	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	const totalClients = 50
	const disconnectClients = 25

	clients := make([]*websocket.Conn, totalClients)
	var wg sync.WaitGroup

	// 2. Spin up 50 mock WebSocket clients
	for i := 0; i < totalClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			dialer := websocket.Dialer{
				HandshakeTimeout: 5 * time.Second,
			}
			conn, _, err := dialer.Dial(wsURL, nil)
			if err != nil {
				t.Errorf("Failed to dial client %d: %v", idx, err)
				return
			}
			clients[idx] = conn
		}(i)
	}
	wg.Wait()

	// Wait a bit for all clients to be registered in the hub
	time.Sleep(200 * time.Millisecond)

	// 3. Begin broadcasting messages at a high frequency
	stopBroadcast := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopBroadcast:
				return
			case <-ticker.C:
				hub.broadcast <- []byte("broadcast message")
			}
		}
	}()

	// Read from the active clients to keep them alive and processing
	for i := 0; i < totalClients-disconnectClients; i++ {
		go func(conn *websocket.Conn) {
			for {
				_, _, err := conn.ReadMessage()
				if err != nil {
					return
				}
			}
		}(clients[i])
	}

	// 4. Abruptly close the network connections of 25 clients without sending a clean WebSocket close frame.
	for i := totalClients - disconnectClients; i < totalClients; i++ {
		clients[i].UnderlyingConn().Close()
	}

	// 5. Wait for the write/read deadlines to expire (e.g., 5-10 seconds).
	// Since we set writeWait to 1s and pongWait to 2s, 3 seconds is plenty.
	time.Sleep(3 * time.Second)

	// Stop broadcasting
	close(stopBroadcast)

	// 6. Assert that the active client count in the Hub drops to 25.
	activeClients := hub.clientCount()
	if activeClients != totalClients-disconnectClients {
		t.Errorf("Expected %d active clients, got %d", totalClients-disconnectClients, activeClients)
	}

	// 7. Assert that runtime.NumGoroutine() returns to the baseline level (plus the remaining 25 active clients' goroutines), confirming no leaked goroutines remain for the disconnected clients.
	var finalGoroutines int
	for i := 0; i < 20; i++ {
		runtime.GC()
		finalGoroutines = runtime.NumGoroutine()
		// We expect baseline + 25 * 3 (75) goroutines.
		// Let's allow a small margin of error (e.g., +/- 10 goroutines) to prevent test flakiness.
		if finalGoroutines <= baseline+75+10 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if finalGoroutines > baseline+75+10 {
		t.Errorf("Goroutine leak detected: baseline %d, expected around %d, got %d", baseline, baseline+75, finalGoroutines)
	}
}
