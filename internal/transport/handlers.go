package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"godynamo/internal/store"
	"godynamo/internal/version"
)

const (
	forwardTimeout = 2 * time.Second
	quorumTimeout  = 2 * time.Second
)

type putRequest struct {
	Value   string              `json:"value"`
	Context version.VectorClock `json:"context,omitempty"`
}

type putResponse struct {
	Clock    version.VectorClock `json:"clock"`
	Replicas int                 `json:"replicas"` // how many held the write
}

type versionDTO struct {
	Value string              `json:"value"`
	Clock version.VectorClock `json:"clock"`
}

type getResponse struct {
	Versions []versionDTO        `json:"versions"`
	Context  version.VectorClock `json:"context"` // echo back on the next write
	Replicas int                 `json:"replicas"`
}

// handlePut coordinates a write: increment the clock, store locally, then
// replicate to the rest of the preference list until W is satisfied.
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	replicas := s.ring.PreferenceList(key, s.cfg.N)
	if len(replicas) == 0 {
		http.Error(w, "ring has no members", http.StatusInternalServerError)
		return
	}
	if coordinator := replicas[0]; coordinator != s.id {
		log.Printf("PUT %s: forwarding to coordinator %s", key, coordinator)
		s.forward(w, r, coordinator)
		return
	}

	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	v, err := store.CoordinatorPut(s.store, key, []byte(req.Value), req.Context, s.id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), quorumTimeout)
	defer cancel()

	// The local write already counts as one, so W-1 more are needed.
	held := 1 + s.replicate(ctx, key, v, replicas[1:], s.cfg.W-1)
	log.Printf("PUT %s: clock=%v held by %d/%d replicas (W=%d)", key, v.Clock, held, len(replicas), s.cfg.W)

	if held < s.cfg.W {
		// The value is durable here even though we report failure -- the
		// client cannot assume either way, which is exactly why writes
		// must be idempotent and carry a context.
		http.Error(w, fmt.Sprintf("write quorum not met: %d of %d replicas", held, s.cfg.W),
			http.StatusServiceUnavailable)
		return
	}

	writeJSON(w, http.StatusOK, putResponse{Clock: v.Clock, Replicas: held})
}

// handleGet coordinates a read: fan out to the whole preference list, wait
// for R answers, then reduce everything seen to the causally current set.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	replicas := s.ring.PreferenceList(key, s.cfg.N)
	if len(replicas) == 0 {
		http.Error(w, "ring has no members", http.StatusInternalServerError)
		return
	}
	if coordinator := replicas[0]; coordinator != s.id {
		log.Printf("GET %s: forwarding to coordinator %s", key, coordinator)
		s.forward(w, r, coordinator)
		return
	}

	local, err := s.store.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), quorumTimeout)
	defer cancel()

	remote, responded := s.gather(ctx, key, replicas[1:], s.cfg.R-1)
	answered := 1 + responded // the local read counts as one

	if answered < s.cfg.R {
		http.Error(w, fmt.Sprintf("read quorum not met: %d of %d replicas", answered, s.cfg.R),
			http.StatusServiceUnavailable)
		return
	}

	all := make([]store.VersionedValue, 0, len(local)+len(remote))
	all = append(all, local...)
	all = append(all, remote...)
	current := store.Reconcile(all)

	log.Printf("GET %s: %d replicas answered, %d version(s) after reconciliation", key, answered, len(current))

	resp := getResponse{
		Versions: make([]versionDTO, len(current)),
		Context:  store.ContextOf(current),
		Replicas: answered,
	}
	for i, v := range current {
		resp.Versions[i] = versionDTO{Value: string(v.Value), Clock: v.Clock}
	}
	writeJSON(w, http.StatusOK, resp)
}

// forward relays r to the node identified by targetID and copies its
// response back to w. handlePut and handleGet both call this identically:
// whichever node a client happens to contact still has to reach the
// coordinator to serve the request, whether it's a write or a read.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, targetID string) {
	addr, ok := s.addrs[targetID]
	if !ok {
		http.Error(w, "unknown coordinator node: "+targetID, http.StatusBadGateway)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Deriving the timeout from r.Context() means that if the original
	// client disconnects, the forwarded request is cancelled too instead
	// of running to completion for no one.
	ctx, cancel := context.WithTimeout(r.Context(), forwardTimeout)
	defer cancel()

	outReq, err := http.NewRequestWithContext(ctx, r.Method, "http://"+addr+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "failed to build forwarded request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	outReq.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(outReq)
	if err != nil {
		http.Error(w, "coordinator "+targetID+" unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
