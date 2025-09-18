package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/db/rft"
	"github.com/InsulaLabs/insi/db/tkv"
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

	es.service.IndEventsOp()

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

// requestSubscriptionSlot attempts to reserve a subscription slot for the given key.
// It performs the action directly if the node is the leader, otherwise it calls the leader's internal API.
// Returns true if the slot was granted, false if the limit was reached, and an error for other failures.
func (c *Core) requestSubscriptionSlot(dataScopeUUID string) (bool, error) {
	if c.fsm.IsLeader() {
		// We are the leader, perform the atomic check-and-increment locally.
		c.subscriptionSlotLock.Lock()
		defer c.subscriptionSlotLock.Unlock()

		limit, current, err := c.getSubscriptionUsage(dataScopeUUID)
		if err != nil {
			return false, fmt.Errorf("could not get subscription usage on leader: %w", err)
		}

		if current >= limit {
			return false, nil // Limit reached
		}

		if err := c.fsm.BumpInteger(WithApiKeySubscriptions(dataScopeUUID), 1); err != nil {
			return false, fmt.Errorf("failed to bump subscription count on leader: %w", err)
		}
		c.logger.Info("Subscription slot granted locally by leader", "ds", dataScopeUUID)
		return true, nil // Slot granted
	}

	// We are a follower, call the leader's internal API.
	leaderInfo, err := c.fsm.LeaderHTTPAddress()
	if err != nil {
		return false, fmt.Errorf("follower could not get leader's address: %w", err)
	}

	payload := subscriptionSlotRequest{DataScopeUUID: dataScopeUUID}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal slot request payload: %w", err)
	}

	_, port, err := net.SplitHostPort(leaderInfo.PrivateBinding)
	if err != nil {
		return false, fmt.Errorf("could not parse leader private binding '%s': %w", leaderInfo.PrivateBinding, err)
	}
	host := "localhost"
	if leaderInfo.ClientDomain != "" {
		host = leaderInfo.ClientDomain
	}
	leaderPrivateURL := fmt.Sprintf("https://%s/db/internal/v1/subscriptions/request_slot", net.JoinHostPort(host, port))

	req, err := http.NewRequest(http.MethodPost, leaderPrivateURL, bytes.NewBuffer(body))
	if err != nil {
		return false, fmt.Errorf("failed to create internal slot request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.loopbackClient.GetApiKey())

	c.logger.Debug("Forwarding subscription slot request to leader", "leader_url", leaderPrivateURL)
	resp, err := c.loopbackClient.GetHttpClient().Do(req)
	if err != nil {
		return false, fmt.Errorf("internal slot request to leader failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		c.logger.Info("Subscription slot granted by leader", "leader_url", leaderPrivateURL)
		return true, nil // Slot granted
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		c.logger.Warn("Subscription slot denied by leader, limit reached", "leader_url", leaderPrivateURL)
		return false, nil // Limit reached
	}

	respBody, _ := io.ReadAll(resp.Body)
	return false, fmt.Errorf("unexpected status code from leader slot request: %d - %s", resp.StatusCode, string(respBody))
}

// releaseSubscriptionSlot tells the leader to decrement the subscription count.
func (c *Core) releaseSubscriptionSlot(dataScopeUUID string) {
	if c.fsm.IsLeader() {
		// We are the leader, decrement locally.
		if err := c.fsm.BumpInteger(WithApiKeySubscriptions(dataScopeUUID), -1); err != nil {
			c.logger.Error("Failed to decrement subscription count on leader", "ds", dataScopeUUID, "error", err)
		}
		return
	}

	// We are a follower, call the leader's internal API.
	// This is a "fire-and-forget" style call, we log errors but don't block/fail.
	go func() {
		leaderInfo, err := c.fsm.LeaderHTTPAddress()
		if err != nil {
			c.logger.Error("Could not get leader address to release slot", "error", err)
			return
		}

		payload := subscriptionSlotRequest{DataScopeUUID: dataScopeUUID}
		body, err := json.Marshal(payload)
		if err != nil {
			c.logger.Error("Failed to marshal slot release payload", "error", err)
			return
		}

		_, port, err := net.SplitHostPort(leaderInfo.PrivateBinding)
		if err != nil {
			c.logger.Error("Could not parse leader private binding for slot release", "binding", leaderInfo.PrivateBinding, "error", err)
			return
		}
		host := "localhost"
		if leaderInfo.ClientDomain != "" {
			host = leaderInfo.ClientDomain
		}
		leaderPrivateURL := fmt.Sprintf("https://%s/db/internal/v1/subscriptions/release_slot", net.JoinHostPort(host, port))

		req, err := http.NewRequest(http.MethodPost, leaderPrivateURL, bytes.NewBuffer(body))
		if err != nil {
			c.logger.Error("Failed to create internal slot release request", "error", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", c.loopbackClient.GetApiKey())

		c.logger.Debug("Forwarding subscription slot release to leader", "leader_url", leaderPrivateURL)
		resp, err := c.loopbackClient.GetHttpClient().Do(req)
		if err != nil {
			c.logger.Error("Internal slot release request to leader failed", "error", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			c.logger.Error("Unexpected status code from leader slot release request", "status", resp.StatusCode, "body", string(respBody))
		} else {
			c.logger.Info("Successfully released subscription slot on leader", "leader_url", leaderPrivateURL)
		}
	}()
}

// eventSubscribeHandler handles WebSocket requests for event subscriptions.
func (c *Core) eventSubscribeHandler(w http.ResponseWriter, r *http.Request) {

	c.IndSubscribersOp()

	// Authentication is handled by ValidateToken, which checks the Authorization header.
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Invalid or missing token", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td, limiterTypeEvents) {
		return
	}

	// Request a subscription slot. This is an atomic operation on the leader.
	slotGranted, err := c.requestSubscriptionSlot(td.DataScopeUUID)
	if err != nil {
		c.logger.Error("Failed to request subscription slot", "key_uuid", td.KeyUUID, "error", err)
		http.Error(w, "internal server error during slot request", http.StatusInternalServerError)
		return
	}

	if !slotGranted {
		c.logger.Warn("Subscription rejected, limit reached", "key_uuid", td.KeyUUID)
		http.Error(w, "subscriber limit exceeded", http.StatusServiceUnavailable)
		return
	}

	// Slot was granted, proceed with WebSocket upgrade. If anything fails from here,
	// we must release the slot.
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		c.logger.Warn("WebSocket connection attempt without topic")
		http.Error(w, "Missing topic", http.StatusBadRequest)
		c.releaseSubscriptionSlot(td.KeyUUID) // Release the slot
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
		c.releaseSubscriptionSlot(td.KeyUUID) // Release the slot
		return
	}
	// Incrementing will be done in registerSubscriber after successful upgrade
	c.wsConnectionLock.Unlock() // Unlock before upgrading, lock again in registerSubscriber

	conn, err := c.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		c.logger.Error("Failed to upgrade WebSocket connection, releasing slot", "error", err, "topic", prefixedTopic, "key_uuid", td.KeyUUID)
		c.releaseSubscriptionSlot(td.KeyUUID) // Release the slot
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
		c.logger.Error("Attempted to register subscriber when max connections already met or exceeded, releasing slot", "active", c.activeWsConnections, "max", c.cfg.Sessions.MaxConnections, "key_uuid", session.keyUUID)
		go session.conn.Close()
		c.releaseSubscriptionSlot(session.keyUUID)
		return
	}
	c.activeWsConnections++
	c.logger.Info("Incremented active WebSocket connections", "count", c.activeWsConnections)

	if _, ok := c.eventSubscribers[session.topic]; !ok {
		c.eventSubscribers[session.topic] = make(map[*eventSession]bool)
	}
	c.eventSubscribers[session.topic][session] = true
	c.logger.Info("Subscriber registered locally", "topic", session.topic, "remote_addr", session.conn.RemoteAddr().String())
}

func (c *Core) unregisterSubscriber(session *eventSession) {
	c.eventSubscribersLock.Lock()
	defer c.eventSubscribersLock.Unlock()

	c.wsConnectionLock.Lock()
	defer c.wsConnectionLock.Unlock()

	if sessionsInTopic, ok := c.eventSubscribers[session.topic]; ok {
		if _, ok := sessionsInTopic[session]; ok {
			delete(c.eventSubscribers[session.topic], session)
			c.logger.Info("Subscriber unregistered, releasing slot", "topic", session.topic, "remote_addr", session.conn.RemoteAddr().String())

			// Release the subscription slot on the leader
			c.releaseSubscriptionSlot(session.keyUUID)

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

// requestSubscriptionSlotHandler is an internal handler called by follower nodes
// to atomically request a subscription slot from the leader.
func (c *Core) requestSubscriptionSlotHandler(w http.ResponseWriter, r *http.Request) {

	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.logger.Error("Non-leader received request for subscription slot. This is a bug and should not happen.")
		http.Error(w, "this node is not the leader", http.StatusServiceUnavailable)
		return
	}

	var req subscriptionSlotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.DataScopeUUID == "" {
		http.Error(w, "data_scope_uuid is missing", http.StatusBadRequest)
		return
	}

	// This is the critical atomic section
	c.subscriptionSlotLock.Lock()
	defer c.subscriptionSlotLock.Unlock()

	limit, current, err := c.getSubscriptionUsage(req.DataScopeUUID)
	if err != nil {
		c.logger.Error("Could not get subscription usage", "ds", req.DataScopeUUID, "error", err)
		http.Error(w, "failed to check usage", http.StatusInternalServerError)
		return
	}

	if current >= limit {
		c.logger.Warn("Subscription slot request denied, limit reached", "ds", req.DataScopeUUID, "current", current, "limit", limit)
		http.Error(w, "subscriber limit exceeded", http.StatusServiceUnavailable)
		return
	}

	// Limit not reached, grant slot by incrementing
	if err := c.fsm.BumpInteger(WithApiKeySubscriptions(req.DataScopeUUID), 1); err != nil {
		c.logger.Error("Failed to bump subscription count", "ds", req.DataScopeUUID, "error", err)
		http.Error(w, "failed to update usage", http.StatusInternalServerError)
		return
	}

	c.logger.Info("Subscription slot granted", "ds", req.DataScopeUUID)
	w.WriteHeader(http.StatusOK)
}

// releaseSubscriptionSlotHandler is an internal handler called by follower nodes
// to release a subscription slot on the leader.
func (c *Core) releaseSubscriptionSlotHandler(w http.ResponseWriter, r *http.Request) {

	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.logger.Error("Non-leader received request to release subscription slot. This is a bug and should not happen.")
		http.Error(w, "this node is not the leader", http.StatusServiceUnavailable)
		return
	}

	var req subscriptionSlotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.DataScopeUUID == "" {
		http.Error(w, "data_scope_uuid is missing", http.StatusBadRequest)
		return
	}

	// Decrement the count. We don't need to lock for a simple decrement,
	// and we log errors but don't fail the request to prevent blocking.
	if err := c.fsm.BumpInteger(WithApiKeySubscriptions(req.DataScopeUUID), -1); err != nil {
		c.logger.Error("Failed to decrement subscription count", "ds", req.DataScopeUUID, "error", err)
	}

	c.logger.Info("Subscription slot released", "ds", req.DataScopeUUID)
	w.WriteHeader(http.StatusOK)
}

// -- WRITE OPERATIONS --

func (c *Core) eventsHandler(w http.ResponseWriter, r *http.Request) {
	// captured in IndEventsOp

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td, limiterTypeEvents) {
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path, rcPublic)
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

	limitStr, err := c.fsm.Get(WithApiKeyMaxEvents(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for events", "ds", td.DataScopeUUID, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for events", "key", td.KeyUUID, "ds", td.DataScopeUUID, "limit", limitStr, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	currentEventsStr, err := c.fsm.Get(WithApiKeyEvents(td.KeyUUID))
	if err != nil && !tkv.IsErrKeyNotFound(err) {
		c.logger.Error("Could not get current events", "key", td.KeyUUID, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	currentEvents, _ := strconv.ParseInt(currentEventsStr, 10, 64)

	if currentEvents >= limit {
		lastResetStr, err := c.fsm.Get(WithApiKeyEventLastReset(td.KeyUUID))
		if err != nil {
			if !tkv.IsErrKeyNotFound(err) {
				c.logger.Error("Could not get last reset time", "key", td.KeyUUID, "error", err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			// Key not found: this is the first time they have hit the limit.
			// Set the timestamp and reject this event. The reset cycle will start now.
			if err := c.fsm.Set(models.KVPayload{
				Key:   WithApiKeyEventLastReset(td.KeyUUID),
				Value: time.Now().UTC().Format(time.RFC3339),
			}); err != nil {
				c.logger.Error("Could not set initial last reset time", "key", td.KeyUUID, "error", err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			w.Header().Set("X-Current-Events", strconv.FormatInt(currentEvents, 10))
			w.Header().Set("X-Events-Limit", limitStr)
			http.Error(w, "Daily event limit reached. Please try again tomorrow.", http.StatusBadRequest)
			return
		}

		lastResetTime, err := time.Parse(time.RFC3339, lastResetStr)
		if err != nil {
			c.logger.Error("Could not parse last reset time", "key", td.KeyUUID, "lastReset", lastResetStr, "error", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		if time.Since(lastResetTime) < 24*time.Hour {
			w.Header().Set("X-Current-Events", strconv.FormatInt(currentEvents, 10))
			w.Header().Set("X-Events-Limit", limitStr)
			http.Error(w, "Daily event limit exceeded. Please try again tomorrow.", http.StatusBadRequest)
			return
		}

		// It has been more than 24 hours. Reset the counters and proceed with the event.
		// 1. Reset the usage tracker to "0". It will be bumped to 1 after this event is published.
		if err := c.fsm.Set(models.KVPayload{
			Key:   WithApiKeyEvents(td.KeyUUID),
			Value: "0",
		}); err != nil {
			c.logger.Error("Could not reset events tracker", "key", td.KeyUUID, "error", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		// 2. Reset the last reset time to now.
		if err := c.fsm.Set(models.KVPayload{
			Key:   WithApiKeyEventLastReset(td.KeyUUID),
			Value: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			c.logger.Error("Could not set last reset time", "key", td.KeyUUID, "error", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
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

	/*
		Increment the current events tracker.
		This will update the value considered the "usage" tracker,
		which is what we compare their max limit to (set by admin)
	*/
	if err := c.fsm.BumpInteger(WithApiKeyEvents(td.KeyUUID), 1); err != nil {
		c.logger.Error("Could not bump integer via FSM for events", "error", err)
		// Don't fail the whole request, but log it.
	}
	w.WriteHeader(http.StatusOK)
}

type subscriptionSlotRequest struct {
	DataScopeUUID string `json:"data_scope_uuid"`
}

func (c *Core) getSubscriptionUsage(keyUUID string) (limit, current int64, err error) {

	// get datascope for key uuid
	dsUUID, err := c.fsm.Get(withApiKeyDataScope(keyUUID))
	if err != nil {
		return 0, 0, fmt.Errorf("could not get data scope: %w", err)
	}

	limitStr, err := c.fsm.Get(WithApiKeyMaxSubscriptions(dsUUID))
	if err != nil {
		return 0, 0, fmt.Errorf("could not get max subscribers limit: %w", err)
	}
	limit, err = strconv.ParseInt(limitStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("could not parse max subscribers limit: %w", err)
	}

	currentSubsStr, err := c.fsm.Get(WithApiKeySubscriptions(dsUUID))
	if err != nil && !tkv.IsErrKeyNotFound(err) {
		return 0, 0, fmt.Errorf("could not get current subscribers: %w", err)
	}
	current, _ = strconv.ParseInt(currentSubsStr, 10, 64)

	return limit, current, nil
}

func (c *Core) eventPurgeHandler(w http.ResponseWriter, r *http.Request) {
	c.IndEventsOp()

	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td, limiterTypeEvents) {
		return
	}

	// Collect all sessions to disconnect
	sessionsToDisconnect := make([]*eventSession, 0)

	c.eventSubscribersLock.Lock()
	for topic, subscribers := range c.eventSubscribers {
		for session := range subscribers {
			if session.keyUUID == td.KeyUUID {
				sessionsToDisconnect = append(sessionsToDisconnect, session)
				c.logger.Info("Found subscriber to purge",
					"topic", topic,
					"key_uuid", td.KeyUUID,
					"remote_addr", session.conn.RemoteAddr().String())
			}
		}
	}
	c.eventSubscribersLock.Unlock()

	// Now disconnect all collected sessions
	// We only close the WebSocket connections, which will trigger readPump to exit
	// and call unregisterSubscriber in its defer, ensuring proper cleanup
	disconnectedCount := len(sessionsToDisconnect)

	for _, session := range sessionsToDisconnect {
		// Close the WebSocket connection - this will cause readPump to exit
		// and trigger proper cleanup via its defer function
		if err := session.conn.Close(); err != nil {
			c.logger.Error("Error closing WebSocket connection during purge",
				"error", err,
				"remote_addr", session.conn.RemoteAddr().String())
		}
	}

	c.logger.Info("Purged event subscriptions",
		"key_uuid", td.KeyUUID,
		"disconnected_count", disconnectedCount)

	// Return success with count of disconnected sessions
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":               true,
		"key_uuid":              td.KeyUUID,
		"disconnected_sessions": disconnectedCount,
	})
}
