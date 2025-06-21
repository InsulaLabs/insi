package core

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/InsulaLabs/insi/badge"
	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/db/rft"
	"github.com/InsulaLabs/insi/db/tkv"
	"github.com/fatih/color"
	"github.com/gorilla/websocket"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/time/rate"
)

type AccessEntity bool

const (
	AccessEntityAnyUser AccessEntity = false // for require root = false
	AccessEntityRoot    AccessEntity = true  // for require root = true
)

const (
	EntityRoot     = "root"
	MemoryUsageKey = "memory-usage"
	DiskUsageKey   = "disk-usage"
)

const MaxValueSize = 1024 * 1024 // 1MB

var ErrDiskUsageLimitExceeded = fmt.Errorf("disk usage limit exceeded")
var ErrMemoryUsageLimitExceeded = fmt.Errorf("memory usage limit exceeded")

var TombstoneRunnerInterval = time.Second * 30

type StorageTarget string

const (
	StorageTargetDisk   StorageTarget = "disk"
	StorageTargetMemory StorageTarget = "memory"
)

/*
	These are in-memory caches that hold ephemeral values for
	common read requests. The idea is that we don't bump the ttl
	of specific caches so we can essentially "buffer" away when
	we actually hit the db.
*/

const (
	CommonStartupTaskDelayDuration = time.Second * 10
)

type Core struct {
	appCtx    context.Context
	cfg       *config.Cluster
	nodeCfg   *config.Node
	nodeName  string // The logical name of this node (e.g. "node0")
	logger    *slog.Logger
	tkv       tkv.TKV
	identity  badge.Badge
	fsm       rft.FSMInstance
	authToken string
	pubMux    *http.ServeMux
	privMux   *http.ServeMux

	startedAt time.Time

	rateLimiters map[string]*ttlcache.Cache[string, *rate.Limiter]

	// WebSocket event handling
	eventSubscribers     map[string]map[*eventSession]bool // Changed to store *eventSession
	eventSubscribersLock sync.RWMutex
	wsUpgrader           websocket.Upgrader
	eventCh              chan models.Event // Central event channel for the service
	activeWsConnections  int32             // Counter for active WebSocket connections
	wsConnectionLock     sync.Mutex        // To protect the activeWsConnections counter

	apiCache *ttlcache.Cache[string, models.TokenData]

	// This is a client that is used in the core of the database to talk to the
	// cluster it is part of :o
	loopbackClient *client.Client

	blobService *blobService

	// Internal event handling
	internalSubscribers     map[string][]func(event models.Event)
	internalSubscribersLock sync.RWMutex
}

type serverInstance struct {
	binding string
	mux     *http.ServeMux
	server  *http.Server
}

func (c *Core) GetRootClientKey() string {
	return c.authToken
}

func (c *Core) tdIsRoot(td models.TokenData) bool {
	return td.KeyUUID == c.cfg.RootPrefix
}

func (c *Core) GetMemoryUsageFullKey() string {
	return fmt.Sprintf("%s:tracking:%s", c.cfg.RootPrefix, MemoryUsageKey)
}

func (c *Core) GetDiskUsageFullKey() string {
	return fmt.Sprintf("%s:tracking:%s", c.cfg.RootPrefix, DiskUsageKey)
}

func (s *Core) AddHandler(path string, handler http.Handler) error {
	if !s.startedAt.IsZero() {
		return fmt.Errorf("service already started, cannot add handler after startup")
	}
	s.pubMux.Handle(path, handler)
	return nil
}

func New(
	ctx context.Context,
	logger *slog.Logger,
	nodeSpecificCfg *config.Node,
	identity badge.Badge,
	tkv tkv.TKV,
	clusterCfg *config.Cluster,
	asNodeId string,
	rootApiKey string,
) (*Core, error) {

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
	rateLimiters := make(map[string]*ttlcache.Cache[string, *rate.Limiter])
	rlLogger := logger.With("component", "rate-limiter")

	makeCategoryRateLimiter := func(category string) *ttlcache.Cache[string, *rate.Limiter] {
		cache := ttlcache.New[string, *rate.Limiter](
			ttlcache.WithTTL[string, *rate.Limiter](time.Minute*1),
			ttlcache.WithDisableTouchOnHit[string, *rate.Limiter](),
		)
		go cache.Start()
		return cache
	}

	if rlConfig := clusterCfg.RateLimiters.Values; rlConfig.Limit > 0 {
		rateLimiters["values"] = makeCategoryRateLimiter("values")
		rlLogger.Info("Initialized rate limiter for 'values'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.Cache; rlConfig.Limit > 0 {
		rateLimiters["cache"] = makeCategoryRateLimiter("cache")
		rlLogger.Info("Initialized rate limiter for 'cacheEndpoints'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.System; rlConfig.Limit > 0 {
		rateLimiters["system"] = makeCategoryRateLimiter("system")
		rlLogger.Info("Initialized rate limiter for 'system'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.Default; rlConfig.Limit > 0 {
		rateLimiters["default"] = makeCategoryRateLimiter("default")
		rlLogger.Info("Initialized rate limiter for 'default'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}
	if rlConfig := clusterCfg.RateLimiters.Events; rlConfig.Limit > 0 {
		rateLimiters["events"] = makeCategoryRateLimiter("events")
		rlLogger.Info("Initialized rate limiter for 'events'", "limit", rlConfig.Limit, "burst", rlConfig.Burst)
	}

	apiCache := ttlcache.New[string, models.TokenData](
		ttlcache.WithTTL[string, models.TokenData](time.Minute*1),

		// Disable touch on hit for for api keys so auto expire
		// can be leveraged for syncronization
		ttlcache.WithDisableTouchOnHit[string, models.TokenData](),
	)
	go apiCache.Start()

	service := &Core{
		appCtx:              ctx,
		cfg:                 clusterCfg,
		nodeCfg:             nodeSpecificCfg,
		nodeName:            asNodeId,
		logger:              logger,
		identity:            identity,
		tkv:                 tkv,
		fsm:                 fsm,
		authToken:           authToken,
		rateLimiters:        rateLimiters,
		pubMux:              http.NewServeMux(),
		privMux:             http.NewServeMux(),
		eventSubscribers:    make(map[string]map[*eventSession]bool),
		internalSubscribers: make(map[string][]func(models.Event)),
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

	// Construct the "loopback" client that we can use internally to talk to the cluster

	endpoints := []client.Endpoint{}

	for _, node := range clusterCfg.Nodes {
		endpoints = append(endpoints, client.Endpoint{
			PublicBinding:  node.PublicBinding,
			PrivateBinding: node.PrivateBinding,
			ClientDomain:   node.ClientDomain,
		})
	}

	service.loopbackClient, err = client.NewClient(&client.Config{
		Logger:         logger,
		ConnectionType: client.ConnectionTypeRandom,
		ApiKey:         rootApiKey,
		SkipVerify:     clusterCfg.ClientSkipVerify,
		Timeout:        time.Second * 30,
		Endpoints:      endpoints,
	})

	if err != nil {
		return nil, err
	}

	blobService, err := newBlobService(logger, service, service.loopbackClient, identity, endpoints)
	if err != nil {
		return nil, err
	}

	service.blobService = blobService
	return service, nil
}

func (c *Core) getRemoteAddress(r *http.Request) string {
	remoteIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		c.logger.Debug("Could not split host and port from remote address", "remote_addr", r.RemoteAddr, "error", err)
		remoteIP = r.RemoteAddr
	}

	trusted := make(map[string]struct{})
	for _, proxy := range c.cfg.TrustedProxies {
		trusted[proxy] = struct{}{}
	}

	for _, node := range c.cfg.Nodes {
		nodeHost, _, err := net.SplitHostPort(node.PublicBinding)
		if err != nil {
			c.logger.Warn("Could not parse node publicBinding", "binding", node.PublicBinding, "error", err)
			continue
		}
		trusted[nodeHost] = struct{}{}
	}

	if _, ok := trusted[remoteIP]; ok {
		if forwardedFor := r.Header.Get("X-Forwarded-For"); forwardedFor != "" {
			ips := strings.Split(forwardedFor, ",")
			clientIP := strings.TrimSpace(ips[0])
			return clientIP
		}
	}
	return remoteIP
}

func (c *Core) getRateLimiter(category string, r *http.Request) *rate.Limiter {
	limiterCategory, ok := c.rateLimiters[category]
	if !ok {
		// Fallback to default if category not found, though this shouldn't happen with proper setup
		limiterCategory = c.rateLimiters["default"]
	}
	ip := c.getRemoteAddress(r)
	limiterItem := limiterCategory.Get(ip)
	if limiterItem == nil {
		var rlConfig config.RateLimiterConfig
		switch category {
		case "values":
			rlConfig = c.cfg.RateLimiters.Values
		case "cache":
			rlConfig = c.cfg.RateLimiters.Cache
		case "system":
			rlConfig = c.cfg.RateLimiters.System
		case "events":
			rlConfig = c.cfg.RateLimiters.Events
		default:
			rlConfig = c.cfg.RateLimiters.Default
		}
		limiter := rate.NewLimiter(rate.Limit(rlConfig.Limit), rlConfig.Burst)
		limiterItem = limiterCategory.Set(ip, limiter, time.Minute*1)
	}
	return limiterItem.Value()
}

func (c *Core) rateLimitMiddleware(next http.Handler, category string) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limiter := c.getRateLimiter(category, r)
		res := limiter.Reserve()
		// If there's a delay, the request is rate-limited.
		if delay := res.Delay(); delay > 0 {
			// We're not proceeding, so cancel the reservation to return the token.
			res.Cancel()
			c.logger.Warn("Rate limit exceeded", "category", category, "path", r.URL.Path, "remote_addr", r.RemoteAddr)

			// Set headers to inform the client about the rate limit.
			retryAfterSeconds := math.Ceil(delay.Seconds())
			w.Header().Set("Retry-After", fmt.Sprintf("%.0f", retryAfterSeconds)) // Correctly format seconds.
			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%v", limiter.Limit()))
			w.Header().Set("X-RateLimit-Burst", fmt.Sprintf("%d", limiter.Burst()))
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (c *Core) ipFilterMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !c.isPermittedIP(r) {
			c.logger.Warn("IP address not permitted", "remote_addr", r.RemoteAddr)
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (c *Core) isPermittedIP(r *http.Request) bool {
	// If no IPs are configured, deny all traffic for security.
	if len(c.cfg.PermittedIPs) == 0 {
		return false
	}

	remoteIP := c.getRemoteAddress(r)

	for _, ip := range c.cfg.PermittedIPs {
		if ip == remoteIP {
			return true
		}
	}

	return false
}

// Run forever until the context is cancelled
func (c *Core) Run() {

	makePublicMux := func(binding string) *serverInstance {

		// Values handlers
		c.pubMux.Handle("/db/api/v1/set", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.setHandler), "values")))
		c.pubMux.Handle("/db/api/v1/get", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.getHandler), "values")))
		c.pubMux.Handle("/db/api/v1/delete", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.deleteHandler), "values")))
		c.pubMux.Handle("/db/api/v1/iterate/prefix", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.iterateKeysByPrefixHandler), "values")))
		c.pubMux.Handle("/db/api/v1/setnx", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.setNXHandler), "values")))
		c.pubMux.Handle("/db/api/v1/cas", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.compareAndSwapHandler), "values")))
		c.pubMux.Handle("/db/api/v1/bump", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.bumpHandler), "values")))

		c.pubMux.Handle("/db/api/v1/blob/set", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.uploadBlobHandler), "values")))
		c.pubMux.Handle("/db/api/v1/blob/get", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.getBlobHandler), "values")))
		c.pubMux.Handle("/db/api/v1/blob/delete", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.deleteBlobHandler), "values")))
		c.pubMux.Handle("/db/api/v1/blob/iterate/prefix", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.iterateBlobKeysByPrefixHandler), "values")))

		// Cache handlers
		c.pubMux.Handle("/db/api/v1/cache/set", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.setCacheHandler), "cache")))
		c.pubMux.Handle("/db/api/v1/cache/get", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.getCacheHandler), "cache")))
		c.pubMux.Handle("/db/api/v1/cache/delete", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.deleteCacheHandler), "cache")))
		c.pubMux.Handle("/db/api/v1/cache/setnx", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.setCacheNXHandler), "cache")))
		c.pubMux.Handle("/db/api/v1/cache/cas", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.compareAndSwapCacheHandler), "cache")))
		c.pubMux.Handle("/db/api/v1/cache/iterate/prefix", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.iterateCacheKeysByPrefixHandler), "cache")))

		// Events handlers
		c.pubMux.Handle("/db/api/v1/events", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.eventsHandler), "events")))
		c.pubMux.Handle("/db/api/v1/events/subscribe", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.eventSubscribeHandler), "events")))

		c.pubMux.Handle("/db/api/v1/ping", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.authedPing), "system")))

		// System handle anyone can call with an api key to get their current usage and limits
		c.pubMux.Handle("/db/api/v1/limits", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.callerLimitsHandler), "system")))

		// Aliases are so a caller can "alias" their api key to a different encoded value that they can use across the public
		// api. They don't need to "get" the api key as we will return it in the response.
		c.pubMux.Handle("/db/api/v1/alias/set", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.setAliasHandler), "system")))
		// Deletes the alias - this way we can disable api access without deleting the root api key (triggering data deletion)
		c.pubMux.Handle("/db/api/v1/alias/delete", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.deleteAliasHandler), "system")))
		// List is so they can get a list of all the aliases they have set for the purpose of deleting or debugging
		c.pubMux.Handle("/db/api/v1/alias/list", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.listAliasesHandler), "system")))

		return &serverInstance{
			binding: binding,
			mux:     c.pubMux,
			server:  nil,
		}
	}

	makePrivateMux := func(binding string) *serverInstance {

		// System handlers
		c.privMux.Handle("/db/api/v1/join", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.joinHandler), "system")))

		// Internal handlers
		c.privMux.Handle("/db/internal/v1/blob/download", c.ipFilterMiddleware(http.HandlerFunc(c.internalDownloadBlobHandler)))

		// Only ROOT can set limits
		c.privMux.Handle("/db/api/v1/admin/limits/set", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.setLimitsHandler), "system")))

		// Admin API Key Management handlers (system category for rate limiting)
		c.privMux.Handle("/db/api/v1/admin/api/create", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.apiKeyCreateHandler), "system")))
		c.privMux.Handle("/db/api/v1/admin/api/delete", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.apiKeyDeleteHandler), "system")))

		// Only ROOT can get limits for specific keys
		c.privMux.Handle("/db/api/v1/admin/limits/get", c.ipFilterMiddleware(c.rateLimitMiddleware(http.HandlerFunc(c.specificLimitsHandler), "system")))

		return &serverInstance{
			binding: binding,
			mux:     c.privMux,
			server:  nil,
		}
	}

	httpListenAddr := c.nodeCfg.PublicBinding
	c.logger.Info("Attempting to start server", "listen_addr", httpListenAddr, "tls_enabled", (c.cfg.TLS.Cert != "" && c.cfg.TLS.Key != ""))

	makeHttpServer := func(srv *serverInstance) *serverInstance {

		srv.server = &http.Server{
			Addr:    srv.binding,
			Handler: srv.mux,
		}

		go func() {
			<-c.appCtx.Done()
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelShutdown()

			if err := srv.server.Shutdown(shutdownCtx); err != nil {
				c.logger.Error("Server shutdown error", "error", err)
			}
		}()

		return srv
	}

	pub := makeHttpServer(makePublicMux(c.nodeCfg.PublicBinding))
	priv := makeHttpServer(makePrivateMux(c.nodeCfg.PrivateBinding))

	/*
	 Kick off the startup tasks
	*/
	go func() {
		time.Sleep(CommonStartupTaskDelayDuration)
		c.ensureRootKeyTrackersExist()

		/*
			Start the tombstone runner AFTER the root key trackers exist
			just to make sure everything is in-order for deletion routines
			It should be fine, but operationally it makes sense to do this
			after the root key trackers exist.
		*/
		go c.tombstoneRunner()
	}()

	/*
		Launch the servers
	*/

	// The blob service MUST be started before the http servers so that
	// all nodes are ready to handle replication events.
	if err := c.blobService.start(c.appCtx); err != nil {
		c.logger.Error("Failed to start blob service, shutting down.", "error", err)
		return
	}

	c.startedAt = time.Now()
	wg := sync.WaitGroup{}

	launch := func(srv *serverInstance) {
		color.HiGreen("Launching server %s", srv.binding)
		wg.Add(1)
		go func() {
			defer wg.Done()

			if c.cfg.TLS.Cert != "" && c.cfg.TLS.Key != "" {
				c.logger.Info("Starting HTTPS server", "cert", c.cfg.TLS.Cert, "key", c.cfg.TLS.Key)
				srv.server.TLSConfig = &tls.Config{}
				if err := srv.server.ListenAndServeTLS(c.cfg.TLS.Cert, c.cfg.TLS.Key); err != http.ErrServerClosed {
					c.logger.Error("HTTPS server error", "error", err)
				}
			} else {
				c.logger.Info("TLS cert or key not specified in config. Starting HTTP server (insecure).")
				if err := srv.server.ListenAndServe(); err != http.ErrServerClosed {
					c.logger.Error("HTTP server error", "error", err)
				}
			}
		}()
	}

	/*
		Signal will kill the servers which will free these contexts
		which will allow the main context to exit
	*/
	launch(pub)
	launch(priv)
	wg.Wait()

	stopWg := sync.WaitGroup{}

	stopWg.Add(1)
	go func() {
		defer stopWg.Done()
		if err := c.blobService.stop(); err != nil {
			c.logger.Error("Error stopping blob service", "error", err)
		}
	}()

	stopWg.Add(1)
	go func() {
		defer stopWg.Done()
		c.apiCache.Stop()
	}()

	stopWg.Add(1)
	go func() {
		defer stopWg.Done()
		for _, subscriber := range c.eventSubscribers {
			for session := range subscriber {
				if session.conn != nil {
					if err := session.conn.Close(); err != nil {
						c.logger.Error("Error closing WebSocket connection", "error", err)
					}
				}
			}
		}
		c.eventSubscribers = make(map[string]map[*eventSession]bool)
	}()

	stopWg.Add(1)
	go func() {
		defer stopWg.Done()
		for _, limiter := range c.rateLimiters {
			limiter.Stop()
		}
	}()

	c.logger.Info("Waiting for server to stop - this may take a moment")
	stopWg.Wait()

	c.logger.Info("Server stopped")
}

func (c *Core) ensureRootKeyTrackersExist() {
	if !c.fsm.IsLeader() {
		return
	}

	keysToSet := []string{
		WithApiKeyMemoryUsage(c.cfg.RootPrefix),
		WithApiKeyDiskUsage(c.cfg.RootPrefix),
		WithApiKeyEvents(c.cfg.RootPrefix),
		WithApiKeySubscriptions(c.cfg.RootPrefix),
	}

	c.logger.Info("Ensuring root key trackers exist", "keys", keysToSet)
	for _, key := range keysToSet {
		val, err := c.fsm.Get(key)
		if tkv.IsErrKeyNotFound(err) {
			c.fsm.Set(models.KVPayload{
				Key:   key,
				Value: "0",
			})
			c.logger.Info("set root tracker", "key", strings.TrimSuffix(key, c.cfg.RootPrefix), "value", "0")
			color.HiRed("set root tracker key %s value %s", strings.TrimSuffix(key, c.cfg.RootPrefix), "0")
		} else {
			color.HiCyan("Key already exists %s %s", strings.TrimSuffix(key, c.cfg.RootPrefix), val)
			c.logger.Info(
				"Key already exists",
				"key",
				strings.TrimSuffix(key, c.cfg.RootPrefix),
				"value",
				val,
			)
		}
	}

	keysToSet = []string{
		WithApiKeyMaxMemoryUsage(c.cfg.RootPrefix),
		WithApiKeyMaxDiskUsage(c.cfg.RootPrefix),
		WithApiKeyMaxEvents(c.cfg.RootPrefix),
		WithApiKeyMaxSubscriptions(c.cfg.RootPrefix),
	}

	for _, key := range keysToSet {
		val, err := c.fsm.Get(key)
		if tkv.IsErrKeyNotFound(err) {
			c.logger.Info("set root tracker", "key", strings.TrimSuffix(key, c.cfg.RootPrefix))
			color.HiRed("set root tracker key %s", strings.TrimSuffix(key, c.cfg.RootPrefix))
			c.fsm.Set(models.KVPayload{
				Key:   key,
				Value: fmt.Sprintf("%d", 1024*1024*1024*10), // 10GB or some wild amount of events/ subscriptions
			})
		} else {
			color.HiCyan("Key already exists %s %s", strings.TrimSuffix(key, c.cfg.RootPrefix), val)
			c.logger.Info(
				"Key already exists",
				"key",
				strings.TrimSuffix(key, c.cfg.RootPrefix),
				"value",
				val,
			)
		}
	}
}

func (c *Core) CheckDiskUsage(td models.TokenData, bytes int64) (ok bool, current string, limit string) {
	limit, err := c.fsm.Get(WithApiKeyMaxDiskUsage(td.KeyUUID))
	if err != nil {
		c.logger.Error("Could not get limit for disk usage", "error", err)
		return false, "0", "0"
	}
	limitInt, err := strconv.ParseInt(limit, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for disk usage", "error", err)
		return false, "0", limit
	}

	current, err = c.fsm.Get(WithApiKeyDiskUsage(td.KeyUUID))
	if err != nil {
		if tkv.IsErrKeyNotFound(err) {
			current = "0"
		} else {
			c.logger.Error("Could not get current disk usage", "error", err)
			return false, "0", limit
		}
	}
	currentInt, err := strconv.ParseInt(current, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current disk usage", "error", err)
		return false, current, limit
	}

	if currentInt+bytes > limitInt {
		return false, current, limit
	}

	return true, current, limit
}

func (c *Core) CheckMemoryUsage(td models.TokenData, bytes int64) (ok bool, current string, limit string) {
	limit, err := c.fsm.Get(WithApiKeyMaxMemoryUsage(td.KeyUUID))
	if err != nil {
		c.logger.Error("Could not get limit for memory usage", "error", err)
		return false, "0", "0"
	}
	limitInt, err := strconv.ParseInt(limit, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for memory usage", "error", err)
		return false, "0", limit
	}

	current, err = c.fsm.Get(WithApiKeyMemoryUsage(td.KeyUUID))
	if err != nil {
		if tkv.IsErrKeyNotFound(err) {
			current = "0"
		} else {
			c.logger.Error("Could not get current memory usage", "error", err)
			return false, "0", limit
		}
	}
	currentInt, err := strconv.ParseInt(current, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current memory usage", "error", err)
		return false, current, limit
	}

	if currentInt+bytes > limitInt {
		return false, current, limit
	}

	return true, current, limit
}

func (c *Core) AssignBytesToTD(td models.TokenData, target StorageTarget, bytes int64) error {
	switch target {
	case StorageTargetDisk:
		if err := c.fsm.BumpInteger(WithApiKeyDiskUsage(td.KeyUUID), int(bytes)); err != nil {
			c.logger.Error("Could not bump disk usage", "error", err)
			return err
		}
	case StorageTargetMemory:
		if err := c.fsm.BumpInteger(WithApiKeyMemoryUsage(td.KeyUUID), int(bytes)); err != nil {
			c.logger.Error("Could not bump memory usage", "error", err)
			return err
		}
	default:
		return fmt.Errorf("invalid storage target: %s", target)
	}
	return nil
}

func sizeTooLargeForStorage(value interface{}) bool {
	return int64(len(fmt.Sprintf("%v", value))) > MaxValueSize
}

func (c *Core) tombstoneRunner() {
	for {
		select {
		case <-c.appCtx.Done():
			return
		case <-time.After(TombstoneRunnerInterval):
			if !c.fsm.IsLeader() {
				continue
			}
			c.execTombstoneDeletion()
		}
	}
}

func (c *Core) execTombstoneDeletion() {
	c.logger.Info("Tombstone runner executing deletion cycle")

	// 1. Iterate over all tombstone keys
	tombstoneKeys, err := c.fsm.Iterate(ApiTombstonePrefix, 0, 100)
	if err != nil {
		c.logger.Error("Could not get tombstone keys", "error", err)
		return
	}

	if len(tombstoneKeys) == 0 {
		c.logger.Info("No tombstones to process")
		return
	}

	c.logger.Info("Found tombstones to process", "count", len(tombstoneKeys))

	for _, tombstoneKeyBytes := range tombstoneKeys {
		if c.appCtx.Err() != nil {
			return // Application is shutting down
		}

		tombstoneKey := string(tombstoneKeyBytes)
		keyUUID := strings.TrimPrefix(tombstoneKey, ApiTombstonePrefix+":")

		dataScopeUUID, err := c.fsm.Get(tombstoneKey)
		if err != nil {
			c.logger.Error("Could not get data scope uuid from tombstone", "tombstone_key", tombstoneKey, "error", err)
			continue
		}

		// 2. Iteratively delete all data associated with the dataScopeUUID
		// We use the dataScopeUUID as the prefix for all user data.
		keysToDelete, err := c.fsm.Iterate(dataScopeUUID, 0, 100)
		if err != nil {
			c.logger.Error("Could not iterate over keys for data scope", "data_scope_uuid", dataScopeUUID, "error", err)
			continue
		}

		if len(keysToDelete) > 0 {
			c.logger.Info("Deleting data for tombstoned key", "key_uuid", keyUUID, "data_scope_uuid", dataScopeUUID, "keys_to_delete_count", len(keysToDelete))
			for _, key := range keysToDelete {
				if err := c.fsm.Delete(string(key)); err != nil {
					c.logger.Error("Could not delete key during tombstone cleanup", "key", string(key), "error", err)
				}
			}

			// If we deleted keys, there might be more. We'll process them in the next run.
			c.logger.Info("Partial data deleted for key. Will continue in next cycle.", "key_uuid", keyUUID)
			continue
		}

		// Clean up aliases associated with this key
		aliasPrefix := WithRootToAliasPrefix(keyUUID)
		// We can use a large limit here as the number of aliases is capped.
		aliasMappingKeys, err := c.fsm.Iterate(aliasPrefix, 0, MaxAliasesPerKey*2)
		if err != nil {
			c.logger.Error("Could not iterate over alias mappings for cleanup", "parent_key_uuid", keyUUID, "error", err)
			// Don't return, still try to clean up the primary key
		}

		if len(aliasMappingKeys) > 0 {
			c.logger.Info("Found aliases to clean up for tombstoned key", "parent_key_uuid", keyUUID, "count", len(aliasMappingKeys))
			for _, aliasMappingKeyBytes := range aliasMappingKeys {
				fullMappingKey := string(aliasMappingKeyBytes)
				// Key format is `internal:root_to_alias:<keyUUID>:<aliasKey>`
				aliasKey := strings.TrimPrefix(fullMappingKey, aliasPrefix+":")

				c.logger.Debug("Deleting alias as part of parent key cleanup", "parent_key_uuid", keyUUID, "alias_key", aliasKey)

				// 1. Delete the alias key itself directly (no tombstone)
				if err := c.deleteApiKeyDirectly(aliasKey); err != nil {
					c.logger.Error("Could not delete alias key directly during cleanup", "alias_key", aliasKey, "error", err)
				}

				// 2. Delete the root -> alias mapping
				if err := c.fsm.Delete(fullMappingKey); err != nil {
					c.logger.Error("Could not delete root-to-alias mapping during cleanup", "mapping_key", fullMappingKey, "error", err)
				}

				// 3. Delete the alias -> root mapping
				if err := c.fsm.Delete(WithAliasToRoot(aliasKey)); err != nil {
					c.logger.Error("Could not delete alias-to-root mapping during cleanup", "alias_key", aliasKey, "error", err)
				}
			}
		}

		// If no more data keys are found, delete the API key metadata and the key itself.
		c.logger.Info("All user data deleted for key. Deleting metadata.", "key_uuid", keyUUID)

		metaKeysToDelete := []string{
			// Trackers
			WithApiKeyMemoryUsage(keyUUID),
			WithApiKeyDiskUsage(keyUUID),
			WithApiKeyEvents(keyUUID),
			WithApiKeySubscriptions(keyUUID),
			// Limits
			WithApiKeyMaxMemoryUsage(keyUUID),
			WithApiKeyMaxDiskUsage(keyUUID),
			WithApiKeyMaxEvents(keyUUID),
			WithApiKeyMaxSubscriptions(keyUUID),
			// The API key itself
			fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, keyUUID),
		}

		for _, key := range metaKeysToDelete {
			if err := c.fsm.Delete(key); err != nil {
				c.logger.Error("Could not delete metadata key", "key", key, "error", err)
				// We continue even if there's an error to attempt to delete as much as possible.
			}
		}

		// 4. Finally, delete the tombstone key itself
		if err := c.fsm.Delete(tombstoneKey); err != nil {
			c.logger.Error("Could not delete tombstone key", "key", tombstoneKey, "error", err)
		} else {
			c.logger.Info("Successfully processed and deleted tombstone", "key_uuid", keyUUID)
		}
	}
}

func (c *Core) SubscribeInternally(topic string, handler func(event models.Event)) {
	c.internalSubscribersLock.Lock()
	defer c.internalSubscribersLock.Unlock()

	c.internalSubscribers[topic] = append(c.internalSubscribers[topic], handler)
	c.logger.Info("New internal subscriber registered", "topic", topic)
}

/*

	Core event processing

*/

func (c *Core) eventProcessingLoop() {
	for {
		select {
		case <-c.appCtx.Done():
			c.logger.Info("Event processing loop stopping.")
			return
		case event := <-c.eventCh:
			c.logger.Debug("Service event loop received event", "topic", event.Topic)

			// --- Handle Internal Subscribers First ---
			c.internalSubscribersLock.RLock()
			if handlers, ok := c.internalSubscribers[event.Topic]; ok {
				c.logger.Debug("Found internal subscribers for topic", "topic", event.Topic, "count", len(handlers))
				for _, handler := range handlers {
					// Launch in a goroutine to avoid blocking the event loop
					go handler(event)
				}
			}
			c.internalSubscribersLock.RUnlock()

			// --- Then Handle WebSocket Subscribers ---
			c.dispatchToWebsocketSubscribers(event)
		}
	}
}

func (c *Core) dispatchToWebsocketSubscribers(event models.Event) {
	c.eventSubscribersLock.RLock()
	defer c.eventSubscribersLock.RUnlock()

	if subscribers, ok := c.eventSubscribers[event.Topic]; ok {
		c.logger.Debug("Found WebSocket subscribers for topic", "topic", event.Topic, "count", len(subscribers))

		// Marshal the event once for all subscribers of this topic.
		message, err := json.Marshal(event)
		if err != nil {
			c.logger.Error("Failed to marshal event for WebSocket dispatch", "topic", event.Topic, "error", err)
			return
		}

		for session := range subscribers {
			// Select to avoid blocking if the send channel is full.
			select {
			case session.send <- message:
				// Event sent successfully.
			default:
				// The client's channel is full. This indicates a slow client.
				c.logger.Warn("Could not send event to client, channel is full", "topic", event.Topic, "remoteAddr", session.conn.RemoteAddr().String())
			}
		}
	} else {
		c.logger.Debug("No WebSocket subscribers for topic in dispatch", "topic", event.Topic)
	}
}
