// Package transport exposes each node's HTTP API. Any node can receive a
// client request for any key; if the receiving node is not that key's
// coordinator, it forwards the request on and relays the coordinator's
// response back to the client.
package transport

import (
	"net/http"

	"godynamo/internal/ring"
	"godynamo/internal/store"
)

// Server is one node's HTTP access point.
type Server struct {
	id     string
	addrs  map[string]string // node ID -> host:port, for forwarding to peers
	ring   *ring.Ring
	store  store.Store
	client *http.Client
}

// NewServer builds a Server for node id. addrs must map every node ID in
// the cluster (including id itself) to its host:port, and must be
// identical across every node's process -- the ring, and therefore who
// coordinates a given key, is computed independently by each node from
// this same member list.
func NewServer(id string, addrs map[string]string, r *ring.Ring, s store.Store) *Server {
	return &Server{
		id:     id,
		addrs:  addrs,
		ring:   r,
		store:  s,
		client: &http.Client{},
	}
}

// Handler returns the HTTP handler for this node's API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	return mux
}
