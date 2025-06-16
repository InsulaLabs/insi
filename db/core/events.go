package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	sendBufferSize = 256                 // Buffer size for the send channel.
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
	// The UUID of the API key that created the session.
	keyUUID string
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
	// Authentication is handled by ValidateToken, which checks the Authorization header.
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Invalid or missing token", http.StatusUnauthorized)
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
		send:    make(chan []byte, sendBufferSize),
		service: c,
		keyUUID: td.KeyUUID,
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
		c.logger.Error("Attempted to register subscriber when max connections already met or exceeded", "active", c.activeWsConnections, "max", c.cfg.Sessions.MaxConnections)
		go session.conn.Close()
		return
	}
	c.activeWsConnections++
	c.logger.Info("Incremented active WebSocket connections", "count", c.activeWsConnections)

	if _, ok := c.eventSubscribers[session.topic]; !ok {
		c.eventSubscribers[session.topic] = make(map[*eventSession]bool)
	}
	c.eventSubscribers[session.topic][session] = true

	if err := c.fsm.BumpInteger(WithApiKeySubscriptions(session.keyUUID), 1); err != nil {
		c.logger.Error("Could not bump integer via FSM for subscribers", "error", err)
		// Don't fail the registration, but log it.
	}

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

			if err := c.fsm.BumpInteger(WithApiKeySubscriptions(session.keyUUID), -1); err != nil {
				c.logger.Error("Could not bump integer via FSM for subscribers on unregister", "error", err)
				// Don't fail, just log.
			}

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
			c.eventSubscribersLock.RLock()
			hasSubscribers := len(c.eventSubscribers) > 0
			c.eventSubscribersLock.RUnlock()

			if !hasSubscribers {
				c.logger.Debug("No active subscribers, skipping event dispatch", "topic", event.Topic)
				continue
			}

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

	sessionsForTopic, ok := c.eventSubscribers[event.Topic]
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
	for session := range sessionsForTopic {
		select {
		case session.send <- message:
			c.logger.Debug("Message queued for WebSocket subscriber", "topic", event.Topic, "remote_addr", session.conn.RemoteAddr())
		default:
			c.logger.Warn("Subscriber send channel full, message dropped", "topic", event.Topic, "remote_addr", session.conn.RemoteAddr())
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
		s.service.logger.Info(
			"WebSocket readPump finished, connection closed and unregistered",
			"remote_addr", s.conn.RemoteAddr(),
			"topic", s.topic,
		)
	}()
	s.conn.SetReadLimit(maxMessageSize)
	s.conn.SetReadDeadline(time.Time{})

	s.conn.SetPongHandler(func(string) error {
		s.service.logger.Debug("WebSocket pong received", "remote_addr", s.conn.RemoteAddr())
		s.conn.SetReadDeadline(time.Time{})
		return nil
	})

	for {
		_, message, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(
				err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				s.service.logger.Error(
					"WebSocket read error",
					"remote_addr",
					s.conn.RemoteAddr(), "topic",
					s.topic, "error", err,
				)
			} else {
				s.service.logger.Info(
					"WebSocket connection closed",
					"remote_addr", s.conn.RemoteAddr(),
					"topic", s.topic, "error", err,
				)
			}
			break
		}
		s.service.logger.Debug(
			"Received message from client on event WebSocket (typically ignored)",
			"remote_addr", s.conn.RemoteAddr(),
			"message_type", message)
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

	limit, err := c.fsm.Get(WithApiKeyMaxEvents(td.KeyUUID))
	if err != nil {
		c.logger.Error("Could not get limit for events", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	limitInt, err := strconv.ParseInt(limit, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for events", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// get the current events
	currentEvents, err := c.fsm.Get(WithApiKeyEvents(td.KeyUUID))
	if err != nil {
		c.logger.Error("Could not get current events", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	currentEventsInt, err := strconv.ParseInt(currentEvents, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current events", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if currentEventsInt+1 > limitInt {
		// Add headers to show them their current events and the limit
		w.Header().Set("X-Current-Events", currentEvents)
		w.Header().Set("X-Events-Limit", limit)
		http.Error(w, "Events limit exceeded", http.StatusBadRequest)
		return
	}

	// Prefix the topic with the entity's UUID to scope it
	prefixedTopic := fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Topic)
	c.logger.Debug(
		"Publishing event with prefixed topic",
		"original_topic", p.Topic,
		"prefixed_topic", prefixedTopic,
		"entity_uuid", td.DataScopeUUID,
	)

	err = c.fsm.Publish(prefixedTopic, p.Data)
	if err != nil {
		c.logger.Error("Could not publish event via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if err := c.fsm.BumpInteger(WithApiKeyEvents(td.KeyUUID), 1); err != nil {
		c.logger.Error("Could not bump integer via FSM for events", "error", err)
		// Don't fail the whole request, but log it.
	}
	w.WriteHeader(http.StatusOK)
}
