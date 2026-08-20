package transport_test

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"godynamo/internal/ring"
	"godynamo/internal/store"
	"godynamo/internal/transport"
)

// listen opens a loopback listener and registers its address under id in
// addrs. Every listener needed by a test must be opened -- and addrs fully
// populated -- before any server goroutine starts, so there is no
// concurrent access to the map: the map is written only by the test
// goroutine, and only ever read afterwards, by goroutines that start later.
func listen(t *testing.T, addrs map[string]string, id string) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	addrs[id] = lis.Addr().String()
	return lis
}

// keyOwnedBy finds a key that the ring assigns to nodeID.
func keyOwnedBy(r *ring.Ring, nodeID string) string {
	for i := 0; ; i++ {
		key := fmt.Sprintf("key-%d", i)
		if r.Owner(key) == nodeID {
			return key
		}
	}
}

func TestServer_ForwardsWriteAndReadToCoordinator(t *testing.T) {
	addrs := make(map[string]string)
	lisA := listen(t, addrs, "A")
	lisB := listen(t, addrs, "B")

	r := ring.New([]string{"A", "B"})
	storeA := store.NewMemory()
	storeB := store.NewMemory()

	srvA := transport.NewServer("A", addrs, r, storeA)
	srvB := transport.NewServer("B", addrs, r, storeB)
	go http.Serve(lisA, srvA.Handler())
	go http.Serve(lisB, srvB.Handler())

	key := keyOwnedBy(r, "B")
	client := &http.Client{Timeout: 2 * time.Second}

	// Send the write to A even though B owns this key -- A must forward.
	putReq, err := http.NewRequest(http.MethodPut, "http://"+addrs["A"]+"/kv/"+key, strings.NewReader(`{"value":"v1"}`))
	if err != nil {
		t.Fatal(err)
	}
	putResp, err := client.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT via non-coordinator A: status %d", putResp.StatusCode)
	}

	// The value must have landed on B (the real coordinator), not A.
	if got, _ := storeB.Get(key); len(got) != 1 || string(got[0].Value) != "v1" {
		t.Fatalf("expected coordinator B to store the value, got %v", got)
	}
	if got, _ := storeA.Get(key); len(got) != 0 {
		t.Fatalf("non-coordinator A should not store the value, got %v", got)
	}

	// Read the same key back through A again -- must also forward.
	getResp, err := client.Get("http://" + addrs["A"] + "/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET via non-coordinator A: status %d", getResp.StatusCode)
	}
	var body struct {
		Versions []struct{ Value string }
	}
	if err := json.NewDecoder(getResp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Versions) != 1 || body.Versions[0].Value != "v1" {
		t.Fatalf("GET via A returned %+v, want [{v1}]", body.Versions)
	}
}

func TestServer_HandlesLocallyWhenAlreadyCoordinator(t *testing.T) {
	addrs := make(map[string]string)
	lisA := listen(t, addrs, "A")
	lisB := listen(t, addrs, "B")
	_ = lisB // never started; A must not need to reach B for its own keys

	r := ring.New([]string{"A", "B"})
	srvA := transport.NewServer("A", addrs, r, store.NewMemory())
	go http.Serve(lisA, srvA.Handler())

	key := keyOwnedBy(r, "A")
	client := &http.Client{Timeout: 2 * time.Second}

	resp, err := client.Get("http://" + addrs["A"] + "/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET for A's own key: status %d (should not have tried to forward)", resp.StatusCode)
	}
}

func TestServer_ForwardFailsWhenCoordinatorUnreachable(t *testing.T) {
	addrs := make(map[string]string)
	lisA := listen(t, addrs, "A")
	lisB := listen(t, addrs, "B")
	lisB.Close() // B is registered but nothing is listening: "down"

	r := ring.New([]string{"A", "B"})
	srvA := transport.NewServer("A", addrs, r, store.NewMemory())
	go http.Serve(lisA, srvA.Handler())

	key := keyOwnedBy(r, "B")
	client := &http.Client{Timeout: 2 * time.Second}

	resp, err := client.Get("http://" + addrs["A"] + "/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
}
