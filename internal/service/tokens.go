package service

import (
	"net/http"

	"github.com/InsulaLabs/insi/models"
)

// Returns the entity name (as encoded by user) and then the uuid generated unique to the key
func (s *Service) ValidateToken(r *http.Request) (models.TokenData, bool) {

	authHeader := r.Header.Get("Authorization")

	if authHeader == s.authToken {
		return models.TokenData{
			Entity: EntityRoot,
			UUID:   s.cfg.RootPrefix,
		}, true
	}

	return models.TokenData{}, false
}
