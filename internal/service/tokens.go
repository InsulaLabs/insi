package service

import (
	"net/http"
)

type TokenData struct {
	Entity string `json:"entity"`
	UUID   string `json:"uuid"`
	ApiKey string `json:"api_key"`
}

// Returns the entity name (as encoded by user) and then the uuid generated unique to the key
func (s *Service) validateToken(r *http.Request) (TokenData, bool) {

	authHeader := r.Header.Get("Authorization")

	if authHeader == s.authToken {
		return TokenData{
			Entity: EntityRoot,
			UUID:   s.cfg.RootPrefix,
			ApiKey: authHeader,
		}, true
	}

	return TokenData{}, false
}
