package service

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/InsulaLabs/insi/internal/config"
	"github.com/InsulaLabs/insi/internal/etok"
	"github.com/InsulaLabs/insi/internal/managers"
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

type localCaches struct {
	apiKeys *ttlcache.Cache[string, string]
}

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

	lcs          *localCaches
	rateLimiters map[string]*rate.Limiter

	securityManager managers.SecurityManager

	// WebSocket event handling
	eventSubscribers     map[string]map[*eventSession]bool // Changed to store *eventSession
	eventSubscribersLock sync.RWMutex
	wsUpgrader           websocket.Upgrader
	eventCh              chan models.Event // Central event channel for the service
	activeWsConnections  int32             // Counter for active WebSocket connections
	wsConnectionLock     sync.Mutex        // To protect the activeWsConnections counter

}

func NewService(
	ctx context.Context,
	logger *slog.Logger,
	nodeSpecificCfg *config.Node,
	identity badge.Badge,
	tkv tkv.TKV,
	clusterCfg *config.Cluster,
	asNodeId string,
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

	caches, err := initLocalCaches(&clusterCfg.Cache)
	if err != nil {
		return nil, err
	}

	secHash := sha256.New()
	secHash.Write([]byte(clusterCfg.InstanceSecret))
	authToken := hex.EncodeToString(secHash.Sum(nil))

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

	rateLimiters["admin"] = rate.NewLimiter(rate.Limit(100), 100)
	rlLogger.Info("Initialized rate limiter for 'admin'", "limit", 100, "burst", 100)

	// Initialize the security manager
	securityManager := etok.New(etok.Config{
		Identity: identity,
		Logger:   logger,
	})

	service := &Service{
		appCtx:          ctx,
		cfg:             clusterCfg,
		nodeCfg:         nodeSpecificCfg,
		logger:          logger,
		identity:        identity,
		tkv:             tkv,
		fsm:             fsm,
		authToken:       authToken,
		lcs:             caches,
		rateLimiters:    rateLimiters,
		mux:             http.NewServeMux(),
		securityManager: securityManager,

		eventSubscribers: make(map[string]map[*eventSession]bool),
		wsUpgrader: websocket.Upgrader{
			ReadBufferSize:  clusterCfg.Sessions.WebSocketReadBufferSize,
			WriteBufferSize: clusterCfg.Sessions.WebSocketWriteBufferSize,
			CheckOrigin: func(r *http.Request) bool {
				logger.Debug("WebSocket CheckOrigin called", "origin", r.Header.Get("Origin"), "host", r.Host)
				return true
			},
		},
		eventCh: serviceEventCh,
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

// Run forever until the context is cancelled
func (s *Service) Run() {

	// Values handlers
	s.mux.Handle("/db/api/v1/set", s.rateLimitMiddleware(http.HandlerFunc(s.setHandler), "values"))
	s.mux.Handle("/db/api/v1/get", s.rateLimitMiddleware(http.HandlerFunc(s.getHandler), "values"))
	s.mux.Handle("/db/api/v1/delete", s.rateLimitMiddleware(http.HandlerFunc(s.deleteHandler), "values"))
	s.mux.Handle("/db/api/v1/iterate/prefix", s.rateLimitMiddleware(http.HandlerFunc(s.iterateKeysByPrefixHandler), "values"))

	// Cache handlers
	s.mux.Handle("/db/api/v1/cache/set", s.rateLimitMiddleware(http.HandlerFunc(s.setCacheHandler), "cacheEndpoints"))
	s.mux.Handle("/db/api/v1/cache/get", s.rateLimitMiddleware(http.HandlerFunc(s.getCacheHandler), "cacheEndpoints"))
	s.mux.Handle("/db/api/v1/cache/delete", s.rateLimitMiddleware(http.HandlerFunc(s.deleteCacheHandler), "cacheEndpoints"))

	// Events handlers
	s.mux.Handle("/db/api/v1/events", s.rateLimitMiddleware(http.HandlerFunc(s.eventsHandler), "events"))
	s.mux.Handle("/db/api/v1/events/subscribe", s.rateLimitMiddleware(http.HandlerFunc(s.eventSubscribeHandler), "events"))

	// ETOK handlers
	s.mux.Handle("/db/api/v1/etok/new", s.rateLimitMiddleware(http.HandlerFunc(s.etokNewHandler), "cacheEndpoints"))
	s.mux.Handle("/db/api/v1/etok/verify", s.rateLimitMiddleware(http.HandlerFunc(s.etokVerifyHandler), "cacheEndpoints"))

	// System handlers
	s.mux.Handle("/db/api/v1/join", s.rateLimitMiddleware(http.HandlerFunc(s.joinHandler), "system"))
	s.mux.Handle("/db/api/v1/new-api-key", s.rateLimitMiddleware(http.HandlerFunc(s.newApiKeyHandler), "system"))
	s.mux.Handle("/db/api/v1/delete-api-key", s.rateLimitMiddleware(http.HandlerFunc(s.deleteApiKeyHandler), "system"))
	s.mux.Handle("/db/api/v1/ping", s.rateLimitMiddleware(http.HandlerFunc(s.authedPing), "system"))

	/*
		TODO: (maybe - not very important)
		// Admin routes
		s.mux.Handle("/db/api/v1/admin/login/view", s.rateLimitMiddleware(http.HandlerFunc(s.adminLoginPageHandler), "admin"))
		s.mux.Handle("/db/api/v1/admin/login", s.rateLimitMiddleware(http.HandlerFunc(s.adminLoginHandler), "admin"))
		s.mux.Handle("/db/api/v1/admin/logout", s.rateLimitMiddleware(http.HandlerFunc(s.adminLogoutHandler), "admin"))
		s.mux.Handle("/db/api/v1/admin/dashboard", s.rateLimitMiddleware(http.HandlerFunc(s.adminDashboardHandler), "admin"))

		s.mux.Handle("/db/api/v1/admin/list-api-keys", s.rateLimitMiddleware(http.HandlerFunc(s.adminListApiKeysHandler), "admin"))
		s.mux.Handle("/db/api/v1/admin/list-nodes", s.rateLimitMiddleware(http.HandlerFunc(s.adminListNodesHandler), "admin"))
		// ALL API CRUD DONE VIA TRADITIONAL ROUTES UI IS FOR VIEWING
	*/

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
}
