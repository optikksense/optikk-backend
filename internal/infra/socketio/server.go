// Package socketio provides a Socket.IO server integrated with the Gin router.
// It exposes a /live namespace for real-time streaming (spans, logs).
package socketio

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	socketio "github.com/googollee/go-socket.io"
	"github.com/googollee/go-socket.io/engineio"
	"github.com/googollee/go-socket.io/engineio/transport"
	"github.com/googollee/go-socket.io/engineio/transport/polling"
	"github.com/googollee/go-socket.io/engineio/transport/websocket"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	// Namespace is the Socket.IO namespace for all live-streaming events.
	Namespace = "/live"
)

// Server wraps a go-socket.io server and manages subscriptions.
type Server struct {
	IO *socketio.Server

	// connDone tracks per-connection cancellation channels.
	// When a client disconnects, the channel is closed.
	connDone map[string]chan struct{}
	mu       sync.Mutex
}

// EmitFunc sends a named event with JSON-serializable data to the client.
type EmitFunc func(event string, data any)

// SubscriptionHandler processes a subscribe event and streams data back.
type SubscriptionHandler struct {
	// Event is the subscribe event name, e.g. "subscribe:spans".
	Event string

	// Handle is called when a client emits the subscribe event.
	// payload is the raw JSON message from the client.
	// emit sends an event+data pair to the client.
	// done is closed when the connection disconnects or the subscription ends.
	Handle func(payload json.RawMessage, emit EmitFunc, done <-chan struct{})
}

// NewServer creates and configures a new Socket.IO server.
func NewServer(allowedOrigins []string) (*Server, error) {
	srv := socketio.NewServer(&engineio.Options{
		Transports: []transport.Transport{
			&polling.Transport{},
			&websocket.Transport{
				CheckOrigin: func(r *http.Request) bool {
					origin := r.Header.Get("Origin")
					if origin == "" || len(allowedOrigins) == 0 {
						return true
					}
					for _, allowed := range allowedOrigins {
						if allowed == "*" || allowed == origin {
							return true
						}
						if strings.HasPrefix(allowed, "*.") {
							suffix := allowed[1:]
							if strings.HasSuffix(origin, suffix) {
								return true
							}
						}
					}
					return false
				},
			},
		},
	})

	s := &Server{
		IO:       srv,
		connDone: make(map[string]chan struct{}),
	}

	srv.OnConnect(Namespace, func(conn socketio.Conn) error {
		logger.L().Info("Socket.IO connected", zap.String("namespace", Namespace), zap.String("conn_id", conn.ID()))
		s.mu.Lock()
		s.connDone[conn.ID()] = make(chan struct{})
		s.mu.Unlock()
		return nil
	})

	srv.OnDisconnect(Namespace, func(conn socketio.Conn, reason string) {
		logger.L().Info("Socket.IO disconnected", zap.String("namespace", Namespace), zap.String("conn_id", conn.ID()), zap.String("reason", reason))
		s.mu.Lock()
		if ch, ok := s.connDone[conn.ID()]; ok {
			close(ch)
			delete(s.connDone, conn.ID())
		}
		s.mu.Unlock()
	})

	srv.OnError(Namespace, func(conn socketio.Conn, err error) {
		if conn != nil {
			logger.L().Error("Socket.IO error", zap.String("namespace", Namespace), zap.String("conn_id", conn.ID()), zap.Error(err))
		} else {
			logger.L().Error("Socket.IO error", zap.String("namespace", Namespace), zap.Error(err))
		}
	})

	return s, nil
}

// getDone returns the done channel for a connection, creating one if missing.
func (s *Server) getDone(connID string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.connDone[connID]
	if !ok {
		ch = make(chan struct{})
		s.connDone[connID] = ch
	}
	return ch
}

// RegisterHandler registers a subscription handler on the /live namespace.
func (s *Server) RegisterHandler(h SubscriptionHandler) {
	s.IO.OnEvent(Namespace, h.Event, func(conn socketio.Conn, msg interface{}) {
		done := s.getDone(conn.ID())

		emit := func(event string, data any) {
			conn.Emit(event, data)
		}

		var payload json.RawMessage
		if msg != nil {
			switch v := msg.(type) {
			case string:
				if v != "" {
					payload = json.RawMessage(v)
				}
			default:
				if b, err := json.Marshal(v); err == nil {
					payload = json.RawMessage(b)
				}
			}
		}

		// Run the handler in a goroutine so it can block (poll loop).
		go h.Handle(payload, emit, done)
	})
}

// Serve starts the Socket.IO server's internal event loop.
func (s *Server) Serve() {
	go func() {
		if err := s.IO.Serve(); err != nil {
			logger.L().Error("Socket.IO serve error", zap.Error(err))
		}
	}()
}

// Close gracefully shuts down the Socket.IO server.
func (s *Server) Close() error {
	return s.IO.Close()
}

// GinMiddleware returns a Gin handler function that serves Socket.IO requests.
// Mount on the engine:
//
//	r.GET("/socket.io/*any", sio.GinMiddleware())
//	r.POST("/socket.io/*any", sio.GinMiddleware())
func (s *Server) GinMiddleware() gin.HandlerFunc {
	return gin.WrapH(s.IO)
}
