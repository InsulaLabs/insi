package service

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/internal/config"
	"github.com/InsulaLabs/insi/internal/rft"
	"github.com/InsulaLabs/insula/security/badge"
	"github.com/InsulaLabs/insula/tkv"
	"github.com/jellydator/ttlcache/v3"
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

	startedAt time.Time

	lcs *localCaches
}

func NewService(
	ctx context.Context,
	logger *slog.Logger,
	nodeSpecificCfg *config.Node,
	identity badge.Badge,
	tkv tkv.TKV,
	config *config.Cluster,
	asNodeId string,
) (*Service, error) {
	fsm, err := rft.New(rft.Settings{
		Ctx:     ctx,
		Logger:  logger.With("service", "rft"),
		Config:  config,
		NodeCfg: nodeSpecificCfg,
		NodeId:  asNodeId,
		TkvDb:   tkv,
	})
	if err != nil {
		return nil, err
	}

	caches, err := initLocalCaches(&config.Cache)
	if err != nil {
		return nil, err
	}

	secHash := sha256.New()
	secHash.Write([]byte(config.InstanceSecret))
	authToken := hex.EncodeToString(secHash.Sum(nil))

	return &Service{
		appCtx:    ctx,
		cfg:       config,
		nodeCfg:   nodeSpecificCfg,
		logger:    logger,
		identity:  identity,
		tkv:       tkv,
		fsm:       fsm,
		authToken: authToken,
		lcs:       caches,
	}, nil
}

// Run forever until the context is cancelled
func (s *Service) Run() {

	// Values handlers
	http.HandleFunc("/db/api/v1/set", s.setHandler)
	http.HandleFunc("/db/api/v1/get", s.getHandler)
	http.HandleFunc("/db/api/v1/delete", s.deleteHandler)
	http.HandleFunc("/db/api/v1/iterate/prefix", s.iterateKeysByPrefixHandler)

	// Tagging handlers
	http.HandleFunc("/db/api/v1/tag", s.tagHandler)
	http.HandleFunc("/db/api/v1/untag", s.untagHandler)
	http.HandleFunc("/db/api/v1/iterate/tags", s.iterateKeysByTagsHandler)

	// Cache handlers
	http.HandleFunc("/db/api/v1/cache/set", s.setCacheHandler)
	http.HandleFunc("/db/api/v1/cache/get", s.getCacheHandler)
	http.HandleFunc("/db/api/v1/cache/delete", s.deleteCacheHandler)

	// System handlers
	http.HandleFunc("/db/api/v1/join", s.joinHandler)
	http.HandleFunc("/db/api/v1/new-api-key", s.newApiKeyHandler)
	http.HandleFunc("/db/api/v1/delete-api-key", s.deleteApiKeyHandler)
	http.HandleFunc("/db/api/v1/ping", s.authedPing)

	httpListenAddr := s.nodeCfg.HttpBinding
	s.logger.Info("Attempting to start server", "listen_addr", httpListenAddr, "tls_enabled", (s.cfg.TLS.Cert != "" && s.cfg.TLS.Key != ""))

	srv := &http.Server{
		Addr: httpListenAddr,
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
