package service

import (
	"net/http"
	"strings"

	"github.com/InsulaLabs/insi/models"
)

// Returns the entity name (as encoded by user) and then the uuid generated unique to the key
func (s *Service) ValidateToken(r *http.Request) (models.TokenData, bool) {

	authHeader := r.Header.Get("Authorization")
	const bearerPrefix = "Bearer "

	// Check if the header starts with "Bearer " and remove it
	token := authHeader
	if strings.HasPrefix(authHeader, bearerPrefix) {
		token = strings.TrimPrefix(authHeader, bearerPrefix)
	}

	if token == s.authToken {
		return models.TokenData{
			Entity: EntityRoot,
			UUID:   s.cfg.RootPrefix,
		}, true
	}

	return models.TokenData{}, false
}
