// Package transport exposes each node's HTTP API. Any node can receive a
// client request for any key; if the receiving node is not that key's
// coordinator, it forwards the request on and relays the coordinator's
// response back to the client.
package transport

import (
	"fmt"
	"net/http"

	"godynamo/internal/ring"
	"godynamo/internal/store"
)

// Config holds the quorum parameters from the Dynamo paper.
//
// The coordinator counts itself toward all three: with N=3, W=2 a write is
// stored locally and then needs one acknowledgement from the other two
// replicas. R+W > N is what makes a read overlap every completed write.
type Config struct {
	N int // replicas per key
	R int // replicas that must answer a read
	W int // replicas that must acknowledge a write
}

func DefaultConfig() Config {
	return Config{N: 3, R: 2, W: 2}
}

// Validate reports whether the configuration is self-consistent. It does
// not reject R+W <= N: running that way is a legitimate experiment, it
// just gives up the read-your-writes overlap.
func (c Config) Validate() error {
	switch {
	case c.N < 1:
		return fmt.Errorf("N must be at least 1, got %d", c.N)
	case c.R < 1 || c.R > c.N:
		return fmt.Errorf("R must be between 1 and N (%d), got %d", c.N, c.R)
	case c.W < 1 || c.W > c.N:
		return fmt.Errorf("W must be between 1 and N (%d), got %d", c.N, c.W)
	}
	return nil
}

// Server is one node's HTTP access point.
type Server struct {
	id     string
	addrs  map[string]string // node ID -> host:port, for reaching peers
	ring   *ring.Ring
	store  store.Store
	cfg    Config
	client *http.Client
}

// NewServer builds a Server for node id. addrs must map every node ID in
// the cluster (including id itself) to its host:port, and must be
// identical across every node's process -- the ring, and therefore who
// coordinates a given key, is computed independently by each node from
// this same member list.
func NewServer(id string, addrs map[string]string, r *ring.Ring, s store.Store, cfg Config) *Server {
	return &Server{
		id:     id,
		addrs:  addrs,
		ring:   r,
		store:  s,
		cfg:    cfg,
		client: &http.Client{},
	}
}

// Handler returns the HTTP handler for this node's API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Client-facing: any node accepts these for any key.
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("GET /kv/{key}", s.handleGet)

	// Node-to-node: place and read a replica, no coordination.
	mux.HandleFunc("PUT /internal/replica/{key}", s.handleReplicaPut)
	mux.HandleFunc("GET /internal/replica/{key}", s.handleReplicaGet)

	return mux
}
