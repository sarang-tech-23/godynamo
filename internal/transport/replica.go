package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"godynamo/internal/store"
	"godynamo/internal/version"
)

// The /internal/replica endpoints are what one node calls on another to
// place or read a replica. They deliberately do NOT consult the ring, do
// NOT forward, and do NOT touch the vector clock: a replica stores exactly
// the version the coordinator hands it. Only a coordinator ever advances a
// clock, which is what keeps clocks small and prevents a forwarding loop.

type replicaValue struct {
	Value string              `json:"value"`
	Clock version.VectorClock `json:"clock"`
}

type replicaGetResponse struct {
	Versions []replicaValue `json:"versions"`
}

func (s *Server) handleReplicaPut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	var req replicaValue
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid replica write: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Store.Put applies the merge rule, so a replica write is idempotent
	// and safe to retry: re-delivering a version already held is a no-op.
	if err := s.store.Put(key, store.VersionedValue{Value: []byte(req.Value), Clock: req.Clock}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleReplicaGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	versions, err := s.store.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := replicaGetResponse{Versions: make([]replicaValue, len(versions))}
	for i, v := range versions {
		resp.Versions[i] = replicaValue{Value: string(v.Value), Clock: v.Clock}
	}
	writeJSON(w, http.StatusOK, resp)
}

// replicaPut places one version on one peer.
func (s *Server) replicaPut(ctx context.Context, node, key string, v store.VersionedValue) error {
	addr, ok := s.addrs[node]
	if !ok {
		return fmt.Errorf("unknown node %q", node)
	}

	body, err := json.Marshal(replicaValue{Value: string(v.Value), Clock: v.Clock})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		"http://"+addr+"/internal/replica/"+key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain so the connection returns to the pool

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("replica %s returned %s", node, resp.Status)
	}
	return nil
}

// replicaGet reads every version one peer holds for key.
func (s *Server) replicaGet(ctx context.Context, node, key string) ([]store.VersionedValue, error) {
	addr, ok := s.addrs[node]
	if !ok {
		return nil, fmt.Errorf("unknown node %q", node)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+addr+"/internal/replica/"+key, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("replica %s returned %s", node, resp.Status)
	}

	var out replicaGetResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	versions := make([]store.VersionedValue, len(out.Versions))
	for i, rv := range out.Versions {
		versions[i] = store.VersionedValue{Value: []byte(rv.Value), Clock: rv.Clock}
	}
	return versions, nil
}
