package rft

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"crypto/sha256"
	"encoding/hex"
	"net"

	"github.com/InsulaLabs/insi/config"

	"github.com/hashicorp/raft"
)

type AutoJoinConfig struct {
	Logger     *slog.Logger
	Ctx        context.Context
	NodeId     string
	ClusterCfg *config.Cluster
	Raft       *raft.Raft
	MyRaftAddr string
}

// New function to handle auto-join logic
func attemptAutoJoin(
	cfg *AutoJoinConfig,
) error {
	leaderNodeId := cfg.ClusterCfg.DefaultLeader
	if cfg.NodeId == leaderNodeId {
		cfg.Logger.Info("Node is the default leader, skipping auto-join attempt.", "node_id", cfg.NodeId)
		return nil
	}

	leaderNodeCfg, ok := cfg.ClusterCfg.Nodes[leaderNodeId]
	if !ok {
		return fmt.Errorf("default leader node '%s' configuration not found in cluster config", leaderNodeId)
	}

	cfg.Logger.Info(
		"Node is not the default leader. Attempting to join leader",
		"node_id", cfg.NodeId,
		"leader_id", leaderNodeId,
		"leader_public_binding", leaderNodeCfg.PublicBinding,
		"leader_client_domain", leaderNodeCfg.ClientDomain,
	)

	// Determine the target host and port for the join URL
	var connectAddr string
	if leaderNodeCfg.ClientDomain != "" {
		_, port, err := net.SplitHostPort(leaderNodeCfg.PublicBinding)
		if err != nil {
			cfg.Logger.Warn(
				"Could not parse port from leader's PublicBinding",
				"public_binding", leaderNodeCfg.PublicBinding,
				"client_domain", leaderNodeCfg.ClientDomain,
				"error", err,
			)
			connectAddr = leaderNodeCfg.PublicBinding // Fallback to full PublicBinding
		} else {
			connectAddr = net.JoinHostPort(leaderNodeCfg.ClientDomain, port)
			cfg.Logger.Info(
				"Auto-join will connect to leader via domain",
				"node_id", cfg.NodeId,
				"leader_id", leaderNodeId,
				"connect_addr", connectAddr,
				"client_domain", leaderNodeCfg.ClientDomain,
			)
		}
	} else {
		connectAddr = leaderNodeCfg.PublicBinding
		cfg.Logger.Info(
			"Auto-join will connect to leader via PublicBinding",
			"node_id", cfg.NodeId,
			"leader_id", leaderNodeId,
			"connect_addr", connectAddr,
		)
	}

	// Construct join URL. Ensure scheme is https if TLS is used.
	scheme := "http"
	if cfg.ClusterCfg.TLS.Cert != "" && cfg.ClusterCfg.TLS.Key != "" {
		scheme = "https"
	}
	joinURL := fmt.Sprintf("%s://%s/db/api/v1/join?followerId=%s&followerAddr=%s",
		scheme, connectAddr, cfg.NodeId, cfg.MyRaftAddr)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	tlsConfig := &tls.Config{}

	if scheme == "https" {
		if cfg.ClusterCfg.ClientSkipVerify {
			cfg.Logger.Info("Client TLS verification is skipped for auto-join as per ClientSkipVerify config.")
			tlsConfig.InsecureSkipVerify = true
		} else {
			// If not skipping verification, and a ClientDomain is provided for the leader, use it for ServerName.
			if leaderNodeCfg.ClientDomain != "" {
				tlsConfig.ServerName = leaderNodeCfg.ClientDomain
				cfg.Logger.Info(
					"Auto-join client TLS verification will use ServerName",
					"client_domain", leaderNodeCfg.ClientDomain,
					"leader_id", leaderNodeId,
				)
			} else {
				cfg.Logger.Warn(
					"Auto-join client TLS verification will use ServerName",
					"leader_id", leaderNodeId,
				)
			}

			// This assumes certificates are signed by a public CA or a CA in the system's trust store.
			cfg.Logger.Info("Auto-join client TLS will use system default CAs for verifying the leader's certificate.")

		}
		httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	for {
		// Check if already part of the cluster
		currentConfiguration := cfg.Raft.GetConfiguration()
		if err := currentConfiguration.Error(); err == nil {
			for _, srv := range currentConfiguration.Configuration().Servers {
				if srv.ID == raft.ServerID(cfg.NodeId) {
					cfg.Logger.Info("Node already part of the Raft configuration. Auto-join completed.", "node_id", cfg.NodeId)
					return nil
				}
			}
		}

		cfg.Logger.Info("Attempting to join leader via", "join_url", joinURL)
		req, err := http.NewRequestWithContext(cfg.Ctx, "GET", joinURL, nil)
		if err != nil {
			cfg.Logger.Error(
				"Failed to create join request to",
				"join_url", joinURL,
				"error", err,
			)
			select {
			case <-time.After(10 * time.Second):
				continue
			case <-cfg.Ctx.Done():
				cfg.Logger.Error("Auto-join cancelled.", "node_id", cfg.NodeId)
				return cfg.Ctx.Err()
			}
		}

		// Add Authorization header
		if cfg.ClusterCfg.InstanceSecret != "" {
			hasher := sha256.New()
			hasher.Write([]byte(cfg.ClusterCfg.InstanceSecret))
			hexEncodedSecret := hex.EncodeToString(hasher.Sum(nil))
			// Base64 encode the hex-encoded secret, similar to runtime/runtime.go
			b64AuthToken := base64.StdEncoding.EncodeToString([]byte(hexEncodedSecret))
			req.Header.Set("Authorization", "Bearer "+b64AuthToken) // Added "Bearer " prefix
			cfg.Logger.Info("Added Authorization header for auto-join.", "node_id", cfg.NodeId)
		} else {
			cfg.Logger.Warn(
				"InstanceSecret is empty, cannot add Authorization header for auto-join. This might lead to join failures if the leader requires auth.",
				"node_id", cfg.NodeId,
			)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			cfg.Logger.Error(
				"Failed to send join request to",
				"join_url", joinURL,
				"error", err,
			)
			select {
			case <-time.After(10 * time.Second):
				continue
			case <-cfg.Ctx.Done():
				cfg.Logger.Error("Auto-join cancelled.", "node_id", cfg.NodeId)
				return cfg.Ctx.Err()
			}
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			cfg.Logger.Info("Successfully joined leader.", "node_id", cfg.NodeId, "leader_id", leaderNodeId)
			return nil
		}

		cfg.Logger.Error(
			"Join attempt failed",
			"join_url", joinURL,
			"status", resp.Status,
			"body", strings.TrimSpace(string(bodyBytes)),
		)
		select {
		case <-time.After(10 * time.Second):
			// continue loop
		case <-cfg.Ctx.Done():
			cfg.Logger.Error("Auto-join cancelled.", "node_id", cfg.NodeId)
			return cfg.Ctx.Err()
		}
	}
}
