package rft

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"crypto/sha256"
	"encoding/hex"

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

	log.Printf("Node %s is not the default leader. Attempting to join leader %s (%s:%s).",
		currentNodeId, leaderNodeId, leaderNodeCfg.Host, leaderNodeCfg.HttpPort)

	// Construct join URL. Ensure scheme is https if TLS is used.
	scheme := "http"
	if clusterCfg.TLS.Cert != "" && clusterCfg.TLS.Key != "" {
		scheme = "https"
	}
	joinURL := fmt.Sprintf("%s://%s:%s/join?followerId=%s&followerAddr=%s",
		scheme, leaderNodeCfg.Host, leaderNodeCfg.HttpPort, currentNodeId, myRaftAddr)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	tlsConfig := &tls.Config{}

	if scheme == "https" {
		if clusterCfg.ClientSkipVerify {
			log.Println("Client TLS verification is skipped for auto-join as per ClientSkipVerify config.")
			tlsConfig.InsecureSkipVerify = true
		} else if clusterCfg.TLS.Cert != "" { // Assuming this cert might be a CA or the leader's cert to trust
			// If ClientSkipVerify is false, the client needs to trust the server's certificate.
			// This assumes clusterCfg.TLS.Cert might be a CA bundle.
			// For a production setup, a proper CA cert for the leader should be used by the client.
			caCertPool := x509.NewCertPool()
			// This path should ideally be a CA certificate that signed the leader's server certificate.
			// If clusterCfg.TLS.Cert is just the leader's own cert (and self-signed), this won't work
			// unless that cert is somehow added as a CA or ClientSkipVerify is true.
			// For simplicity, we use the provided Cert path. If it's a path to a CA bundle, this is okay.
			caCertBytes, err := os.ReadFile(clusterCfg.TLS.Cert)
			if err != nil {
				log.Printf("Warning: Failed to read configured TLS cert at %s for auto-join CA: %v. Proceeding without custom CA.", clusterCfg.TLS.Cert, err)
				// Potentially fall back to system CAs or require ClientSkipVerify
			} else {
				if !caCertPool.AppendCertsFromPEM(caCertBytes) {
					log.Printf("Warning: Failed to append configured TLS cert at %s to CA pool for auto-join. Proceeding without custom CA.", clusterCfg.TLS.Cert)
				} else {
					tlsConfig.RootCAs = caCertPool
					log.Printf("Auto-join client TLS configured with CA cert: %s", clusterCfg.TLS.Cert)
				}
			}
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
		resp.Body.Close() // Important to close the body

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
