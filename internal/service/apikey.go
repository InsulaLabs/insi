package service

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/InsulaLabs/insi/models"
)

func (s *Service) apiKeyCreateHandler(w http.ResponseWriter, r *http.Request) {
	_, ok := s.ValidateToken(r, true)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for api key create request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.ApiKeyCreateRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Could not unmarshal api key create request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	key, err := s.spawnNewApiKey(req.KeyName)
	if err != nil {
		s.logger.Error("Could not create api key", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	resp := models.ApiKeyCreateResponse{
		KeyName: req.KeyName,
		Key:     key,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // Explicitly set 200 OK
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		// Log error if encoding fails, though headers might have already been sent
		s.logger.Error("Failed to encode API key create response", "error", err)
	}
}

func (s *Service) apiKeyDeleteHandler(w http.ResponseWriter, r *http.Request) {
	_, ok := s.ValidateToken(r, true)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for api key delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.ApiKeyDeleteRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Could not unmarshal api key delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	if err := s.deleteExistingApiKey(req.Key); err != nil {
		s.logger.Error("Could not delete api key", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"success": "true",
	})
}
