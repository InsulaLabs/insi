package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/db/rft"
	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second    // Time allowed to write a message to the peer.
	pongWait       = 60 * time.Second    // Time allowed to read the next pong message from the peer.
	pingPeriod     = (pongWait * 9) / 10 // Send pings to peer with this period. Must be less than pongWait.
	maxMessageSize = 512                 // Maximum message size allowed from peer.
)

// A session of someone connected wanting to receive events from one of the topics
type eventSession struct {
	conn *websocket.Conn
	// The topic this session is subscribed to.
	topic string
	// Buffered channel of outbound messages.
	send chan []byte
	// Service pointer to access logger, etc.
	service *Core
}

type eventSubsystem struct {
	service *Core
	eventCh chan models.Event
	// We don't need to manage subscribers directly here anymore,
	// the Service will handle dispatch based on its eventSubscribers map.
}

var _ rft.EventReceiverIF = &eventSubsystem{}

/*
Satisfies the rft.EventReceiverIF interface so we can retrieve "Fresh" events
from the FSM as they are applied to the network

As events come in this function is called once per-node per-event. So, any subscribers to the event system
that would be connected over websockets to this node address can have the event forwarded to them
*/
func (es *eventSubsystem) Receive(topic string, data any) error {
	// Instead of directly putting to es.eventCh, we now also handle dispatching
	// to WebSocket subscribers from the service layer.
	// The eventCh is still useful for the FSM->Service notification.

	event := models.Event{
		Topic: topic,
		Data:  data,
	}

	// The service's event loop (to be created) will read from es.eventCh
	// and then use es.service.dispatchToSubscribers.
	// For now, let's assume a simple direct dispatch or a new method in service.
	// This design means the FSM (via rft.EventReceiverIF) signals the service,
	// and the service handles the fan-out.

	// Send to the service's central event channel first.
	// A separate goroutine in the Service will pick this up and dispatch.
	select {
	case es.eventCh <- event:
		es.service.logger.Debug("Event placed on service event channel", "topic", topic)
	default:
		es.service.logger.Warn("Service event channel full, event dropped (this should not happen in normal operation)", "topic", topic)
		return fmt.Errorf("event channel full for topic %s", topic)
	}

	return nil
}

// eventSubscribeHandler handles WebSocket requests for event subscriptions.
func (c *Core) eventSubscribeHandler(w http.ResponseWriter, r *http.Request) {
	// Authentication: Extract token from query parameter
	// Example: /db/api/v1/events/subscribe?topic=MY_TOPIC&token=API_KEY
	token := r.URL.Query().Get("token")
	if token == "" {
		c.logger.Warn("WebSocket connection attempt without token")
		http.Error(w, "Missing token", http.StatusUnauthorized)
		return
	}

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		c.logger.Warn("WebSocket connection attempt without topic")
		http.Error(w, "Missing topic", http.StatusBadRequest)
		return
	}

	// Prefix the topic with the entity's UUID to scope it
	prefixedTopic := fmt.Sprintf("%s:%s", td.DataScopeUUID, topic)
	c.logger.Debug("Subscription request for prefixed topic", "original_topic", topic, "prefixed_topic", prefixedTopic, "entity_uuid", td.DataScopeUUID)

	c.wsConnectionLock.Lock()
	if c.activeWsConnections >= int32(c.cfg.Sessions.MaxConnections) {
		c.wsConnectionLock.Unlock()
		c.logger.Warn("Max WebSocket connections reached, rejecting new connection", "current", c.activeWsConnections, "max", c.cfg.Sessions.MaxConnections)
		http.Error(w, "Too many connections", http.StatusServiceUnavailable)
		return
	}
	// Incrementing will be done in registerSubscriber after successful upgrade
	c.wsConnectionLock.Unlock() // Unlock before upgrading, lock again in registerSubscriber

	conn, err := c.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		c.logger.Error("Failed to upgrade WebSocket connection", "error", err, "topic", prefixedTopic)
		return
	}
	c.logger.Info("WebSocket connection upgraded", "remote_addr", conn.RemoteAddr().String(), "topic", prefixedTopic)

	session := &eventSession{
		conn:    conn,
		topic:   prefixedTopic,
		send:    make(chan []byte, 256), // Buffered channel
		service: c,
	}

	c.registerSubscriber(session)

	// Launch goroutines for this session
	go session.writePump()
	go session.readPump()
}

func (c *Core) registerSubscriber(session *eventSession) {
	c.eventSubscribersLock.Lock()
	defer c.eventSubscribersLock.Unlock()

	c.wsConnectionLock.Lock()
	defer c.wsConnectionLock.Unlock()

	if c.activeWsConnections >= int32(c.cfg.Sessions.MaxConnections) {
		// This is a secondary check, primary check is in eventSubscribeHandler
		// If reached here, it's a race condition or an issue. Log and don't register.
		c.logger.Error("Attempted to register subscriber when max connections already met or exceeded", "active", c.activeWsConnections, "max", c.cfg.Sessions.MaxConnections)
		// We should not proceed to add the session if the limit is hit. Close the connection that was just upgraded.
		go session.conn.Close() // Close it in a goroutine to avoid blocking here
		return
	}
	c.activeWsConnections++
	c.logger.Info("Incremented active WebSocket connections", "count", c.activeWsConnections)

	if _, ok := c.eventSubscribers[session.topic]; !ok {
		c.eventSubscribers[session.topic] = make(map[*eventSession]bool) // Inner map stores *eventSession
	}
	c.eventSubscribers[session.topic][session] = true // Store the session itself

	c.logger.Info("Subscriber registered", "topic", session.topic, "remote_addr", session.conn.RemoteAddr().String())
}

func (c *Core) unregisterSubscriber(session *eventSession) {
	c.eventSubscribersLock.Lock()
	defer c.eventSubscribersLock.Unlock()

	c.wsConnectionLock.Lock()
	defer c.wsConnectionLock.Unlock()

	if sessionsInTopic, ok := c.eventSubscribers[session.topic]; ok {
		if _, ok := sessionsInTopic[session]; ok {
			delete(c.eventSubscribers[session.topic], session)
			c.logger.Info("Subscriber unregistered", "topic", session.topic, "remote_addr", session.conn.RemoteAddr().String())

			// Decrement connection count only if we actually found and removed the session
			if c.activeWsConnections > 0 {
				c.activeWsConnections--
				c.logger.Info("Decremented active WebSocket connections", "count", c.activeWsConnections)
			} else {
				c.logger.Warn("Attempted to decrement active WebSocket connections below zero")
			}

			if len(c.eventSubscribers[session.topic]) == 0 {
				delete(c.eventSubscribers, session.topic)
				c.logger.Info("No more subscribers for topic, removing topic from map", "topic", session.topic)
			}
		}
	}
	close(session.send)
}

// Central event processing loop for the service.
// Reads from FSM-populated eventCh and dispatches to WebSocket subscribers.
func (c *Core) eventProcessingLoop() {
	c.logger.Info("Starting service event processing loop for WebSocket dispatch")
	for {
		select {
		case event := <-c.eventCh:
			c.logger.Debug("Service event loop received event", "topic", event.Topic)
			c.dispatchEventToSubscribersViaSessionSend(event)

		case <-c.appCtx.Done():
			c.logger.Info("Service event processing loop shutting down")
			return
		}
	}
}

// dispatchEventToSubscribersViaSessionSend sends an event to all relevant subscriber sessions.
func (c *Core) dispatchEventToSubscribersViaSessionSend(event models.Event) {
	c.eventSubscribersLock.RLock()
	defer c.eventSubscribersLock.RUnlock()

	sessionsForTopic, ok := c.eventSubscribers[event.Topic] // Changed variable name for clarity
	if !ok {
		c.logger.Debug("No WebSocket subscribers for topic in dispatchViaSessionSend", "topic", event.Topic)
		return
	}

	if len(sessionsForTopic) == 0 {
		c.logger.Debug("Zero WebSocket subscribers in map for topic (dispatchViaSessionSend)", "topic", event.Topic)
		return
	}

	c.logger.Debug("Dispatching event via session send channels", "topic", event.Topic, "subscriber_count", len(sessionsForTopic))

	message, err := json.Marshal(event)
	if err != nil {
		c.logger.Error("Failed to marshal event for WebSocket dispatch (dispatchViaSessionSend)", "topic", event.Topic, "error", err)
		return
	}

	// POTENTIAL BOTTLENECK:
	// The for loop in dispatchEventToSubscribersViaSessionSend iterates sequentially
	// through all subscribers for a given topic to queue the event message.
	// If an event has a very large number of subscribers (e.g., thousands),
	// this sequential iteration, even with non-blocking sends to session.send,
	// could make dispatchEventToSubscribersViaSessionSend take a significant
	// amount of time to complete. This, in turn, can slow down the main
	// eventProcessingLoop, potentially causing the central eventCh to fill up if
	// events are being produced by the FSM at a high rate.
	//
	// POTENTIAL SOLUTION OVERVIEW:
	// To parallelize the queuing of messages to subscriber send channels, one could
	// consider:
	// 1. Launching a new goroutine for each subscriber within the loop:
	//    for session := range sessionsForTopic {
	//        go func(s *eventSession, msg []byte) {
	//            select {
	//            case s.send <- msg:
	//                // log success
	//            default:
	//                // log drop, potentially close connection
	//            }
	//        }(session, message) // Pass session and message as args
	//    }
	//    This has the overhead of goroutine creation/scheduling for each subscriber.
	// 2. Using a fixed-size worker pool: Dispatch tasks (to send a message to a
	//    specific session's 'send' channel) to a pool of worker goroutines. This
	//    can limit the number of concurrent dispatch operations and reuse goroutines.
	//
	// Such changes would need careful consideration of goroutine management, error
	// handling, and potential impacts on overall system complexity and resource usage.

	for session := range sessionsForTopic { // session is now *eventSession correctly
		select {
		case session.send <- message:
			c.logger.Debug("Message queued for WebSocket subscriber", "topic", event.Topic, "remote_addr", session.conn.RemoteAddr())
		default:
			c.logger.Warn("Subscriber send channel full, message dropped", "topic", event.Topic, "remote_addr", session.conn.RemoteAddr())
			// To prevent a stuck writer, we might close the connection here.
			// This will trigger readPump to exit and unregister.
			// Be careful with closing from a different goroutine.
			// A common pattern is to launch the close in a new goroutine to avoid deadlock if session.conn.Close() blocks
			// or if unregisterSubscriber (called by readPump) tries to acquire the same lock.
			// go session.conn.Close() // Consider implications
		}
	}
}

// readPump pumps messages from the WebSocket connection to the hub.
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (s *eventSession) readPump() {
	defer func() {
		s.service.unregisterSubscriber(s)
		s.conn.Close()
		s.service.logger.Info("WebSocket readPump finished, connection closed and unregistered", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic)
	}()
	s.conn.SetReadLimit(maxMessageSize)
	// s.conn.SetReadDeadline(time.Now().Add(pongWait)) // Set initial read deadline
	// As per user request: "does _not_ timeout from inactivity" for the client-side subscription
	// So, we should not set a read deadline here based on pongs, or make it very long.
	// The client will send pings, and our pong handler (if any, or default) will reply.
	// If the client doesn't send pings and the connection drops, ReadMessage will error.
	s.conn.SetReadDeadline(time.Time{}) // No read deadline initially.

	s.conn.SetPongHandler(func(string) error { // APP defined pong
		s.service.logger.Debug("WebSocket pong received", "remote_addr", s.conn.RemoteAddr())
		// s.conn.SetReadDeadline(time.Now().Add(pongWait)) // Reset read deadline
		s.conn.SetReadDeadline(time.Time{}) // Keep no read deadline
		return nil
	})

	for {
		// If SetReadDeadline is time.Time{}, ReadMessage will block indefinitely until a message or error.
		_, message, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				s.service.logger.Error("WebSocket read error", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic, "error", err)
			} else {
				s.service.logger.Info("WebSocket connection closed", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic, "error", err)
			}
			break // Exit loop on error (including clean close)
		}
		// Messages from client on this WebSocket are typically ignored for a subscribe-only connection.
		// We could handle client-side pings if they come as messages, or other control messages.
		s.service.logger.Debug("Received message from client on event WebSocket (typically ignored)", "remote_addr", s.conn.RemoteAddr(), "message_type", message)
	}
}

// writePump pumps messages from the hub to the WebSocket connection.
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (s *eventSession) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		s.conn.Close() // Ensure connection is closed if writePump exits
		s.service.logger.Info("WebSocket writePump finished", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic)
		// Unregistration should happen in readPump's defer, or if writePump errors significantly.
	}()
	for {
		select {
		case message, ok := <-s.send:
			s.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				s.service.logger.Info("WebSocket send channel closed by hub", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic)
				s.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := s.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				s.service.logger.Error("WebSocket NextWriter error", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic, "error", err)
				return
			}
			_, err = w.Write(message)
			if err != nil {
				s.service.logger.Error("WebSocket message write error", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic, "error", err)
				// Do not return here, try to close writer
			}

			// Add queued chat messages to the current WebSocket message.
			// This is an optimization for chatty applications, may not be strictly needed here.
			// n := len(s.send)
			// for i := 0; i < n; i++ {
			//  w.Write([]byte{'\n'})
			// 	w.Write(<-s.send)
			// }

			if err := w.Close(); err != nil {
				s.service.logger.Error("WebSocket writer close error", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic, "error", err)
				return
			}
		case <-ticker.C:
			s.conn.SetWriteDeadline(time.Now().Add(writeWait))
			s.service.logger.Debug("WebSocket sending ping", "remote_addr", s.conn.RemoteAddr())
			if err := s.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				s.service.logger.Error("WebSocket ping write error", "remote_addr", s.conn.RemoteAddr(), "topic", s.topic, "error", err)
				return
			}
		case <-s.service.appCtx.Done():
			s.service.logger.Info("Service context done, closing WebSocket connection from writePump", "remote_addr", s.conn.RemoteAddr())
			// Don't send CloseMessage here if readPump will also do it on appCtx.Done via its own select.
			// Or, ensure only one path sends it. Typically, readPump closing triggers unregister, which closes send chan, which makes writePump exit.
			return

		}
	}
}

// -- WRITE OPERATIONS --

func (c *Core) eventsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for events request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.Event
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		c.logger.Error("Invalid JSON payload for events request", "error", err)
		http.Error(w, "Invalid JSON payload for events: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Prefix the topic with the entity's UUID to scope it
	prefixedTopic := fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Topic)
	c.logger.Debug("Publishing event with prefixed topic", "original_topic", p.Topic, "prefixed_topic", prefixedTopic, "entity_uuid", td.DataScopeUUID)

	err = c.fsm.Publish(prefixedTopic, p.Data)
	if err != nil {
		c.logger.Error("Could not publish event via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}
