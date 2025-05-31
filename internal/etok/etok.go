package etok

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/InsulaLabs/insi/internal/managers"
	"github.com/InsulaLabs/insula/security/badge"
	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
)

type Config struct {
	Identity badge.Badge
	Logger   *slog.Logger
}

type tokCacheItem struct {
	requestedScopes map[string]string
	signature       badge.SignedPackage
	signedAt        time.Time
	entityRequester string
	apiUuidOfEntity string
}

type etok struct {
	identity badge.Badge
	logger   *slog.Logger

	instanceUUID    string
	prefixSignedAt  string
	prefixSignature string

	tokCache *ttlcache.Cache[string, tokCacheItem]
}

func New(config Config) *etok {
	instanceUUID := uuid.New().String()

	prefixSignedAt := fmt.Sprintf("%s:signed_at", instanceUUID)
	prefixSignature := fmt.Sprintf("%s:signature", instanceUUID)

	tokCache := ttlcache.New[string, tokCacheItem](
		ttlcache.WithDisableTouchOnHit[string, tokCacheItem](),
		ttlcache.WithTTL[string, tokCacheItem](time.Hour*1),
	)

	go tokCache.Start()

	return &etok{
		identity:        config.Identity,
		logger:          config.Logger,
		instanceUUID:    instanceUUID,
		prefixSignedAt:  prefixSignedAt,
		prefixSignature: prefixSignature,
		tokCache:        tokCache,
	}
}

var _ managers.SecurityManager = &etok{}

func (e *etok) NewScopeToken(entityRequester string, apiUuidOfEntity string, scopes map[string]string, ttl time.Duration) (string, error) {

	if ttl == 0 {
		return "", fmt.Errorf("ttl must be greater than 0")
	}

	if entityRequester == "" {
		return "", fmt.Errorf("entityRequester must be set")
	}

	if apiUuidOfEntity == "" {
		return "", fmt.Errorf("apiUuidOfEntity must be set")
	}

	if len(scopes) == 0 {
		return "", fmt.Errorf("scopes must be set")
	}

	requestString := ""
	for k, v := range scopes {
		requestString += fmt.Sprintf("%s:%s;", k, v)
	}

	signature, err := e.identity.Sign(&requestString)
	if err != nil {
		return "", err
	}

	reqTokenId := uuid.New().String()

	// add to cache
	e.tokCache.Set(reqTokenId, tokCacheItem{
		requestedScopes: scopes,
		signature:       signature,
		signedAt:        time.Now(),
		entityRequester: entityRequester,
		apiUuidOfEntity: apiUuidOfEntity,
	}, ttl)

	return reqTokenId, nil
}

func (e *etok) VerifyScopeToken(token string) (string, string, map[string]string, error) {

	item := e.tokCache.Get(token)
	if item == nil {
		return "", "", nil, fmt.Errorf("token not found")
	}

	// IMPORTANT: We need to delete to be "secure" by design.
	// If we don't delete, then an attacker could reuse the token
	// to gain access to the entity.
	e.tokCache.Delete(token)

	value := item.Value()

	requestedScopes := value.requestedScopes
	entityRequester := value.entityRequester
	apiUuidOfEntity := value.apiUuidOfEntity

	valid, err := e.identity.Verify(value.signature)
	if err != nil {
		// Sanity check to ensure that we got something out of memory that we actually
		// put there ( for if we distribute this across nodes)
		return "", "", nil, fmt.Errorf("signature verification failed: %w", err)
	}

	if !valid {
		return "", "", nil, fmt.Errorf("signature verification failed")
	}

	return entityRequester, apiUuidOfEntity, requestedScopes, nil
}
