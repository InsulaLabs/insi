package etok

/*

	etok/new => give data, get a uuid
	etok/verify => give uuid, get data

		- Deletes value on read, TTL expires, no bump
*/

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/runtime"
	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
)

type TokenRequest struct {
	Data []byte        `json:"data"`
	TTL  time.Duration `json:"ttl"`
}

type TokenResponse struct {
	Token string `json:"token"`
}

type VerifyRequest struct {
	Token string `json:"token"`
}

type VerifyResponse struct {
	Verified bool   `json:"verified"`
	Data     []byte `json:"data"`
}

type EtokPlugin struct {
	logger    *slog.Logger
	prif      runtime.PluginRuntimeIF
	startedAt time.Time

	cache *ttlcache.Cache[string, string]
}

var _ runtime.Plugin = &EtokPlugin{}

func New(logger *slog.Logger) *EtokPlugin {

	cache := ttlcache.New[string, string](
		ttlcache.WithTTL[string, string](1*time.Hour),
		ttlcache.WithDisableTouchOnHit[string, string](),
	)
	go cache.Start()

	return &EtokPlugin{
		logger: logger,
		cache:  cache,
	}
}

func (p *EtokPlugin) GetName() string {
	return "etok"
}

func (p *EtokPlugin) Init(prif runtime.PluginRuntimeIF) *runtime.PluginImplError {
	p.prif = prif
	return nil
}

func (p *EtokPlugin) GetRoutes() []runtime.PluginRoute {
	// No routes for static plugin
	return []runtime.PluginRoute{
		{
			Path:    "new",
			Handler: http.HandlerFunc(p.newHandler),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "verify",
			Handler: http.HandlerFunc(p.verifyHandler),
			Limit:   10,
			Burst:   10,
		},
	}
}

func (p *EtokPlugin) newHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.TTL == 0 {
		http.Error(w, "TTL is required", http.StatusBadRequest)
		return
	}

	if req.TTL > 1*time.Hour {
		http.Error(w, "TTL must be less than 1 hour", http.StatusBadRequest)
		return
	}

	if len(req.Data) > 1024*1024*10 {
		http.Error(w, "Data must be less than 10MB", http.StatusBadRequest)
		return
	}

	token := uuid.New().String()
	p.cache.Set(token, string(req.Data), req.TTL)

	response := TokenResponse{
		Token: token,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		p.logger.Error("failed to encode token response", "error", err)
	}
}

func (p *EtokPlugin) verifyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req VerifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	token := req.Token

	cacheItem := p.cache.Get(token)
	if cacheItem == nil {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	p.cache.Delete(token)

	data := cacheItem.Value()

	response := VerifyResponse{
		Verified: true,
		Data:     []byte(data),
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		p.logger.Error("failed to encode verification response", "error", err)
	}
}
