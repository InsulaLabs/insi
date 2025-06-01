package rft

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"crypto/sha256"
	"encoding/hex"
	"net"

	"github.com/InsulaLabs/insi/config"

	"github.com/hashicorp/raft"
)

// New function to handle auto-join logic
func attemptAutoJoin(
	ctx context.Context,
	currentNodeId string,
	clusterCfg *config.Cluster,
	raftInstance *raft.Raft,
	myRaftAddr string,
) error {
	leaderNodeId := clusterCfg.DefaultLeader
	if currentNodeId == leaderNodeId {
		log.Printf("Node %s is the default leader, skipping auto-join attempt.", currentNodeId)
		return nil
	}

	leaderNodeCfg, ok := clusterCfg.Nodes[leaderNodeId]
	if !ok {
		return fmt.Errorf("default leader node '%s' configuration not found in cluster config", leaderNodeId)
	}

	log.Printf("Node %s is not the default leader. Attempting to join leader %s (%s) clientDomain: %s.",
		currentNodeId, leaderNodeId, leaderNodeCfg.HttpBinding, leaderNodeCfg.ClientDomain)

	// Determine the target host and port for the join URL
	var connectAddr string
	log.Printf("leaderNodeCfg.ClientDomain: %s", leaderNodeCfg.ClientDomain)
	if leaderNodeCfg.ClientDomain != "" {
		_, port, err := net.SplitHostPort(leaderNodeCfg.HttpBinding)
		if err != nil {
			log.Printf("Warning: Could not parse port from leader's HttpBinding '%s' when using ClientDomain '%s'. Falling back to HttpBinding for URL. Error: %v",
				leaderNodeCfg.HttpBinding, leaderNodeCfg.ClientDomain, err)
			connectAddr = leaderNodeCfg.HttpBinding // Fallback to full HttpBinding
		} else {
			connectAddr = net.JoinHostPort(leaderNodeCfg.ClientDomain, port)
			log.Printf("Node %s: Auto-join will connect to leader %s via domain: %s (derived from ClientDomain '%s' and HttpBinding port)",
				currentNodeId, leaderNodeId, connectAddr, leaderNodeCfg.ClientDomain)
		}
	} else {
		connectAddr = leaderNodeCfg.HttpBinding
		log.Printf("Node %s: Auto-join will connect to leader %s via HttpBinding: %s (ClientDomain not set for leader)",
			currentNodeId, leaderNodeId, connectAddr)
	}

	// Construct join URL. Ensure scheme is https if TLS is used.
	scheme := "http"
	if clusterCfg.TLS.Cert != "" && clusterCfg.TLS.Key != "" {
		scheme = "https"
	}
	joinURL := fmt.Sprintf("%s://%s/db/api/v1/join?followerId=%s&followerAddr=%s",
		scheme, connectAddr, currentNodeId, myRaftAddr)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	tlsConfig := &tls.Config{}

	if scheme == "https" {
		if clusterCfg.ClientSkipVerify {
			log.Println("Client TLS verification is skipped for auto-join as per ClientSkipVerify config.")
			tlsConfig.InsecureSkipVerify = true
		} else {
			// If not skipping verification, and a ClientDomain is provided for the leader, use it for ServerName.
			if leaderNodeCfg.ClientDomain != "" {
				tlsConfig.ServerName = leaderNodeCfg.ClientDomain
				log.Printf("Auto-join client TLS verification will use ServerName: %s for leader %s", leaderNodeCfg.ClientDomain, leaderNodeId)
			} else {
				log.Printf("Warning: Auto-join client TLS verification is active, but no ClientDomain specified for leader %s (%s). TLS will verify against the IP/hostname in HttpBinding: %s.", leaderNodeId, leaderNodeCfg.HttpBinding, leaderNodeCfg.HttpBinding)
			}

			// REMOVED CA LOADING LOGIC: System's default CA pool will be used.
			// This assumes certificates are signed by a public CA or a CA in the system's trust store.
			log.Println("Auto-join client TLS will use system default CAs for verifying the leader's certificate.")

		}
		httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	for {
		// Check if already part of the cluster
		currentConfiguration := raftInstance.GetConfiguration()
		if err := currentConfiguration.Error(); err == nil {
			for _, srv := range currentConfiguration.Configuration().Servers {
				if srv.ID == raft.ServerID(currentNodeId) {
					log.Printf("Node %s already part of the Raft configuration. Auto-join completed.", currentNodeId)
					return nil
				}
			}
		}

		log.Printf("Node %s: Attempting to join leader via %s", currentNodeId, joinURL)
		req, err := http.NewRequestWithContext(ctx, "GET", joinURL, nil)
		if err != nil {
			log.Printf("Node %s: Failed to create join request to %s: %v. Retrying in 10s.", currentNodeId, joinURL, err)
			select {
			case <-time.After(10 * time.Second):
				continue
			case <-ctx.Done():
				log.Printf("Node %s: Auto-join cancelled.", currentNodeId)
				return ctx.Err()
			}
		}

		// Add Authorization header
		if clusterCfg.InstanceSecret != "" {
			hasher := sha256.New()
			hasher.Write([]byte(clusterCfg.InstanceSecret))
			authToken := hex.EncodeToString(hasher.Sum(nil))
			req.Header.Set("Authorization", authToken)
			log.Printf("Node %s: Added Authorization header for auto-join.", currentNodeId)
		} else {
			log.Printf("Node %s: InstanceSecret is empty, cannot add Authorization header for auto-join. This might lead to join failures if the leader requires auth.", currentNodeId)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			log.Printf("Node %s: Failed to send join request to %s: %v. Retrying in 10s.", currentNodeId, joinURL, err)
			select {
			case <-time.After(10 * time.Second):
				continue
			case <-ctx.Done():
				log.Printf("Node %s: Auto-join cancelled.", currentNodeId)
				return ctx.Err()
			}
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			log.Printf("Node %s: Successfully joined leader %s.", currentNodeId, leaderNodeId)
			return nil
		}

		log.Printf("Node %s: Join attempt to %s failed. Status: %s, Body: '%s'. Retrying in 10s.",
			currentNodeId, joinURL, resp.Status, strings.TrimSpace(string(bodyBytes)))
		select {
		case <-time.After(10 * time.Second):
			// continue loop
		case <-ctx.Done():
			log.Printf("Node %s: Auto-join cancelled.", currentNodeId)
			return ctx.Err()
		}
	}
}
