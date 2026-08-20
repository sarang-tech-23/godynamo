package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"godynamo/internal/store"
	"godynamo/internal/version"
)

const forwardTimeout = 2 * time.Second

type putRequest struct {
	Value   string              `json:"value"`
	Context version.VectorClock `json:"context,omitempty"`
}

type putResponse struct {
	Clock version.VectorClock `json:"clock"`
}

type versionDTO struct {
	Value string              `json:"value"`
	Clock version.VectorClock `json:"clock"`
}

type getResponse struct {
	Versions []versionDTO `json:"versions"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if coordinator := s.ring.Owner(key); coordinator != s.id {
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

	writeJSON(w, http.StatusOK, putResponse{Clock: v.Clock})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if coordinator := s.ring.Owner(key); coordinator != s.id {
		s.forward(w, r, coordinator)
		return
	}

	versions, err := s.store.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := getResponse{Versions: make([]versionDTO, len(versions))}
	for i, v := range versions {
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
