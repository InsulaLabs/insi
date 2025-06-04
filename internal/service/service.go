package service

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/internal/rft"
	"github.com/InsulaLabs/insi/internal/tkv"
	"github.com/InsulaLabs/insi/models"
	"github.com/InsulaLabs/insula/security/badge"
	"github.com/gorilla/websocket"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/time/rate"
)

const (
	EntityRoot = "root"
)

/*
	These are in-memory caches that hold ephemeral values for
	common read requests. The idea is that we don't bump the ttl
	of specific caches so we can essentially "buffer" away when
	we actually hit the db.
*/

type Service struct {
	appCtx    context.Context
	cfg       *config.Cluster
	nodeCfg   *config.Node
	logger    *slog.Logger
	tkv       tkv.TKV
	identity  badge.Badge
	fsm       rft.FSMInstance
	authToken string
	mux       *http.ServeMux

	startedAt time.Time

	rateLimiters map[string]*rate.Limiter

	// WebSocket event handling
	eventSubscribers     map[string]map[*eventSession]bool // Changed to store *eventSession
	eventSubscribersLock sync.RWMutex
	wsUpgrader           websocket.Upgrader
	eventCh              chan models.Event // Central event channel for the service
	activeWsConnections  int32             // Counter for active WebSocket connections
	wsConnectionLock     sync.Mutex        // To protect the activeWsConnections counter

	apiCache *ttlcache.Cache[string, models.TokenData]
}

func (s *Service) GetRootClientKey() string {
	return s.authToken
}

func (s *Service) AddHandler(path string, handler http.Handler) error {
	if !s.startedAt.IsZero() {
		return fmt.Errorf("service already started, cannot add handler after startup")
	}
	s.mux.Handle(path, handler)
	return nil
}

func NewService(
	ctx context.Context,
	logger *slog.Logger,
	nodeSpecificCfg *config.Node,
	identity badge.Badge,
	tkv tkv.TKV,
	clusterCfg *config.Cluster,
	asNodeId string,
	rootApiKey string,
) (*Service, error) {

	// This eventCh is for the FSM to signal the service.
	serviceEventCh := make(chan models.Event, clusterCfg.Sessions.EventChannelSize) // Buffered channel

	// Satisfies the rft.EventReceiverIF interface so we can retrieve "Fresh" events
	// from the FSM as they are applied to the network. When the FSM gives us an event
	// to hand out to subscribers, we first place it in the eventCh channel
	// and the system that handles connected clients will pull from this channel
	// and forward the event to the client.
	es := &eventSubsystem{
		eventCh: serviceEventCh, // eventSubsystem will use the service's channel
	}

	fsm, err := rft.New(rft.Settings{
		Ctx:           ctx,
		Logger:        logger.With("service", "rft"),
		Config:        clusterCfg,
		NodeCfg:       nodeSpecificCfg,
		NodeId:        asNodeId,
		TkvDb:         tkv,
		EventReceiver: es,
	})
	if err != nil {
		return nil, err
	}

	authToken := rootApiKey

	// Initialize rate limiters
	rateLimiters := make(map[string]*rate.Limiter)
	rlLogger := logger.With("component", "rate-limiter")

	if rlConfig := clusterCfg.RateLimiters.Values; rlConfig.Limit > 0 {
		rateLimiters["values"] = rate.NewLimiter(rate.Limit(rlConfig.Limit), rlConfig.Burst)
		rlLogger.Info("Initialized rate limiter for 'values'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.Cache; rlConfig.Limit > 0 {
		rateLimiters["cacheEndpoints"] = rate.NewLimiter(rate.Limit(rlConfig.Limit), rlConfig.Burst)
		rlLogger.Info("Initialized rate limiter for 'cacheEndpoints'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.System; rlConfig.Limit > 0 {
		rateLimiters["system"] = rate.NewLimiter(rate.Limit(rlConfig.Limit), rlConfig.Burst)
		rlLogger.Info("Initialized rate limiter for 'system'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.Default; rlConfig.Limit > 0 {
		rateLimiters["default"] = rate.NewLimiter(rate.Limit(rlConfig.Limit), rlConfig.Burst)
		rlLogger.Info("Initialized rate limiter for 'default'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.Events; rlConfig.Limit > 0 {
		rateLimiters["events"] = rate.NewLimiter(rate.Limit(rlConfig.Limit), rlConfig.Burst)
		rlLogger.Info("Initialized rate limiter for 'events'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.Queues; rlConfig.Limit > 0 {
		rateLimiters["queues"] = rate.NewLimiter(rate.Limit(rlConfig.Limit), rlConfig.Burst)
		rlLogger.Info("Initialized rate limiter for 'queues'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}

	apiCache := ttlcache.New[string, models.TokenData](
		ttlcache.WithTTL[string, models.TokenData](time.Minute*1),

		// Disable touch on hit for for api keys so auto expire
		// can be leveraged for syncronization
		ttlcache.WithDisableTouchOnHit[string, models.TokenData](),
	)
	go apiCache.Start()

	service := &Service{
		appCtx:           ctx,
		cfg:              clusterCfg,
		nodeCfg:          nodeSpecificCfg,
		logger:           logger,
		identity:         identity,
		tkv:              tkv,
		fsm:              fsm,
		authToken:        authToken,
		rateLimiters:     rateLimiters,
		mux:              http.NewServeMux(),
		eventSubscribers: make(map[string]map[*eventSession]bool),
		wsUpgrader: websocket.Upgrader{
			ReadBufferSize:  clusterCfg.Sessions.WebSocketReadBufferSize,
			WriteBufferSize: clusterCfg.Sessions.WebSocketWriteBufferSize,
			CheckOrigin: func(r *http.Request) bool {
				logger.Debug("WebSocket CheckOrigin called", "origin", r.Header.Get("Origin"), "host", r.Host)
				return true
			},
		},
		eventCh:  serviceEventCh,
		apiCache: apiCache,
	}

	// Set the event subsystem to the service for event logic
	es.service = service

	go service.eventProcessingLoop()

	return service, nil
}

func (s *Service) rateLimitMiddleware(next http.Handler, category string) http.Handler {
	limiter, ok := s.rateLimiters[category]
	if !ok {
		// Fallback to default limiter if category-specific one isn't found or configured
		limiter, ok = s.rateLimiters["default"]
		if !ok { // If no default limiter, then no rate limiting for this handler
			s.logger.Warn("No rate limiter configured for category and no default limiter present", "category", category)
			return next
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !limiter.Allow() {
			s.logger.Warn("Rate limit exceeded", "category", category, "path", r.URL.Path, "remote_addr", r.RemoteAddr)
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Service) WithRoute(path string, handler http.Handler, limit int, burst int) {
	limiter := rate.NewLimiter(rate.Limit(limit), burst)
	s.mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !limiter.Allow() {
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
		handler.ServeHTTP(w, r)
	}))
}

// Run forever until the context is cancelled
func (s *Service) Run() {

	// Values handlers
	s.mux.Handle("/db/api/v1/set", s.rateLimitMiddleware(http.HandlerFunc(s.setHandler), "values"))
	s.mux.Handle("/db/api/v1/get", s.rateLimitMiddleware(http.HandlerFunc(s.getHandler), "values"))
	s.mux.Handle("/db/api/v1/delete", s.rateLimitMiddleware(http.HandlerFunc(s.deleteHandler), "values"))
	s.mux.Handle("/db/api/v1/iterate/prefix", s.rateLimitMiddleware(http.HandlerFunc(s.iterateKeysByPrefixHandler), "values"))

	// Batch Value handlers
	s.mux.Handle("/db/api/v1/batchset", s.rateLimitMiddleware(http.HandlerFunc(s.batchSetHandler), "values"))
	s.mux.Handle("/db/api/v1/batchdelete", s.rateLimitMiddleware(http.HandlerFunc(s.batchDeleteHandler), "values"))

	// Atomic Operation handlers (using "values" rate limiting category for now)
	s.mux.Handle("/db/api/v1/atomic/new", s.rateLimitMiddleware(http.HandlerFunc(s.atomicNewHandler), "values"))
	s.mux.Handle("/db/api/v1/atomic/get", s.rateLimitMiddleware(http.HandlerFunc(s.atomicGetHandler), "values"))
	s.mux.Handle("/db/api/v1/atomic/add", s.rateLimitMiddleware(http.HandlerFunc(s.atomicAddHandler), "values"))
	s.mux.Handle("/db/api/v1/atomic/delete", s.rateLimitMiddleware(http.HandlerFunc(s.atomicDeleteHandler), "values"))

	// Queue handlers (using "queues" rate limiting category)
	s.mux.Handle("/db/api/v1/queue/new", s.rateLimitMiddleware(http.HandlerFunc(s.queueNewHandler), "queues"))
	s.mux.Handle("/db/api/v1/queue/push", s.rateLimitMiddleware(http.HandlerFunc(s.queuePushHandler), "queues"))
	s.mux.Handle("/db/api/v1/queue/pop", s.rateLimitMiddleware(http.HandlerFunc(s.queuePopHandler), "queues"))
	s.mux.Handle("/db/api/v1/queue/delete", s.rateLimitMiddleware(http.HandlerFunc(s.queueDeleteHandler), "queues"))

	// Cache handlers
	s.mux.Handle("/db/api/v1/cache/set", s.rateLimitMiddleware(http.HandlerFunc(s.setCacheHandler), "cache"))
	s.mux.Handle("/db/api/v1/cache/get", s.rateLimitMiddleware(http.HandlerFunc(s.getCacheHandler), "cache"))
	s.mux.Handle("/db/api/v1/cache/delete", s.rateLimitMiddleware(http.HandlerFunc(s.deleteCacheHandler), "cache"))

	// Events handlers
	s.mux.Handle("/db/api/v1/events", s.rateLimitMiddleware(http.HandlerFunc(s.eventsHandler), "events"))
	s.mux.Handle("/db/api/v1/events/subscribe", s.rateLimitMiddleware(http.HandlerFunc(s.eventSubscribeHandler), "events"))

	// System handlers
	s.mux.Handle("/db/api/v1/join", s.rateLimitMiddleware(http.HandlerFunc(s.joinHandler), "system"))
	s.mux.Handle("/db/api/v1/ping", s.rateLimitMiddleware(http.HandlerFunc(s.authedPing), "system"))

	// Admin API Key Management handlers (system category for rate limiting)
	s.mux.Handle("/db/api/v1/admin/api/create", s.rateLimitMiddleware(http.HandlerFunc(s.apiKeyCreateHandler), "system"))
	s.mux.Handle("/db/api/v1/admin/api/delete", s.rateLimitMiddleware(http.HandlerFunc(s.apiKeyDeleteHandler), "system"))

	httpListenAddr := s.nodeCfg.HttpBinding
	s.logger.Info("Attempting to start server", "listen_addr", httpListenAddr, "tls_enabled", (s.cfg.TLS.Cert != "" && s.cfg.TLS.Key != ""))

	srv := &http.Server{
		Addr:    httpListenAddr,
		Handler: s.mux,
	}

	go func() {
		<-s.appCtx.Done()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("Server shutdown error", "error", err)
		}
	}()

	s.startedAt = time.Now()

	if s.cfg.TLS.Cert != "" && s.cfg.TLS.Key != "" {
		s.logger.Info("Starting HTTPS server", "cert", s.cfg.TLS.Cert, "key", s.cfg.TLS.Key)
		srv.TLSConfig = &tls.Config{}
		if err := srv.ListenAndServeTLS(s.cfg.TLS.Cert, s.cfg.TLS.Key); err != http.ErrServerClosed {
			s.logger.Error("HTTPS server error", "error", err)
		}
	} else {
		s.logger.Info("TLS cert or key not specified in config. Starting HTTP server (insecure).")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", "error", err)
		}
	}

	s.apiCache.Stop()
}

// Whenever a cache entry expires, not from deletion, but from TTL expiration this is called
func (s *Service) OnTTLCacheEviction(key string, value string) error {

	// Only leader should modify the counter on eviction
	// the others can just let it die
	if !s.fsm.IsLeader() {
		fmt.Println("OnTTLCacheEviction not leader, skipping")
		return nil
	}

	fmt.Println("OnTTLCacheEviction", key, value)
	return nil
}
