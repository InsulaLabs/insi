package managers

import "time"

type SecurityManager interface {
	NewScopeToken(
		entityRequester string,
		apiUuidOfEntity string,
		scopes map[string]string,
		ttl time.Duration,
	) (string, error)
	VerifyScopeToken(token string) (string, string, map[string]string, error)
}
