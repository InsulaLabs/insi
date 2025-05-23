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
)

type Service struct {
	appCtx    context.Context
	cfg       *config.Cluster
	nodeCfg   *config.Node
	logger    *slog.Logger
	tkv       tkv.TKV
	identity  badge.Badge
	fsm       rft.FSMInstance
	authToken string
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
	}, nil
}

// Run forever until the context is cancelled
func (s *Service) Run() {

	// Values handlers
	http.HandleFunc("/set", s.setHandler)
	http.HandleFunc("/get", s.getHandler)
	http.HandleFunc("/delete", s.deleteHandler)
	http.HandleFunc("/iterate/prefix", s.iterateKeysByPrefixHandler)

	// Tagging handlers
	http.HandleFunc("/tag", s.tagHandler)
	http.HandleFunc("/untag", s.untagHandler)
	http.HandleFunc("/iterate/tags", s.iterateKeysByTagsHandler)

	// Cache handlers
	http.HandleFunc("/cache/set", s.setCacheHandler)
	http.HandleFunc("/cache/get", s.getCacheHandler)
	http.HandleFunc("/cache/delete", s.deleteCacheHandler)

	// System handlers
	http.HandleFunc("/join", s.joinHandler)

	httpListenAddr := ":" + s.nodeCfg.HttpPort
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
