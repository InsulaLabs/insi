package blob

import (
	"context"
	"log/slog"
	"time"

	"github.com/InsulaLabs/insi/badge"
	"github.com/InsulaLabs/insi/client"
)

const (
	TopicBlobUploaded              = "_:blob:uploaded"
	TopicBlobPeerHeartbeat         = "_:blob:peer:heartbeat"
	TopicBlobPeerChallengeRequest  = "_:blob:peer:challenge:request"
	TopicBlobPeerChallengeResponse = "_:blob:peer:challenge:response"

	// idea is to put node identity here and then we can concat the blob id then map to hash. then we can iterate over the node to see if they own the blob
	KeyPrimitiveBlobInstance          = "_:blob:instance:"            // _:blob:instance:node_id:blob_id => record struct { original name, owner uuid, uploaded time, size, hash, etc }
	KeyPrimitiveBlobTombstone         = "_:blob:tombstone:"           // _:blob:tombstone:blob_id => record struct { deleted time, deleted by uuid } ; only first node to receive needs to set to db. then all nodes runners can clean it up.
	KeyPrimitiveBlobTombstoneNodeFlag = "_:blob:tombstone:node_flag:" // _:blob:tombstone:node_flag:blob_id:node_id => timestamp ; last node to delete MUST clean all tombstones. we use node flag to indicate when a node marks complete. if on complete, we iterate over the flags for a tombstone and all nodes are done, delete the tombstone.
	// along with tombstones the last node to delete MUST clean the "instance" record. This will finalize deletion of the blob across the cluster.

	KeyPrefixBlobROS = "_:blob:ros:" // _:blob:ros:node_id

	KeyPrefixNodeIdentityChallenge = "_:node:identity:challenge:" // _:node:identity:challenge:node_id => UUID(random) when connecting to a peer we present our node id, they must return the UUID from the db to confirm they have access to the same level of data.

	// How often we challenge eachother
	ChallengeCycleFrequency = 10 * time.Second
)

type PeerChallengeRequest struct {
	UUID              string
	RequesterIdentity string // challenger
	TargetIdentity    string // challengee
}

type PeerChallengeResponse struct {
	NodeId string
}

type peer struct {
	binding        string
	nodeId         string
	lastSeen       time.Time // last time we got a hearbeat FROM them
	lastChallenged time.Time // last time they proved data access to us
	endpoint       client.Endpoint
}

type Service struct {
	logger     *slog.Logger
	insiClient *client.Client
	identity   badge.Badge // this nodes crypto identity
	peers      []peer
}

func New(logger *slog.Logger, insiClient *client.Client, identity badge.Badge, peers []client.Endpoint) (*Service, error) {

	peerList := []peer{}

	for _, endpoint := range peers {
		peerList = append(peerList, peer{
			binding:        endpoint.PublicBinding,
			nodeId:         endpoint.ClientDomain,
			endpoint:       endpoint,
			lastSeen:       time.Time{},
			lastChallenged: time.Time{},
		})
	}

	return &Service{
		logger:     logger,
		insiClient: insiClient,
		identity:   identity,
		peers:      peerList,
	}, nil
}

func (x *Service) Start(ctx context.Context) error {
	x.startEventSystem(ctx)

	return nil
}

func (x *Service) Stop() error {

	return nil
}

func (x *Service) startEventSystem(ctx context.Context) error {

	go x.insiClient.SubscribeToEvents(TopicBlobUploaded, ctx, func(data any) {
		//
	})

	go x.insiClient.SubscribeToEvents(TopicBlobPeerHeartbeat, ctx, func(data any) {
		// get the event and update the peer last seen time if and only if the last challenge time is < 10 seconds
	})

	return nil
}

func (x *Service) startTombstoneSystem(ctx context.Context) error {

	// start the trheads on the ctx to, ever 5 minutes run the tombstone cleanup described above.
	return nil
}

func (x *Service) startPeerSystem(ctx context.Context) error {

	return nil
}

func (x *Service) startIdentitySystem(ctx context.Context) error {

	return nil
}
