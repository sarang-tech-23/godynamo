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

// cluster is a set of in-process nodes, each on its own real loopback
// listener. Nodes talk to each other over genuine TCP, so the transport
// code under test is exercised exactly as it would be across processes.
type cluster struct {
	addrs  map[string]string
	stores map[string]*store.Memory
	ring   *ring.Ring
}

// startCluster brings up one node per id, skipping any id in `down` --
// those get an address registered but nothing listening on it, which is
// how a failed node looks to its peers.
//
// Every listener is opened and every address registered before any server
// goroutine starts, so the shared addrs map is written only by the test
// goroutine and read only afterwards: no synchronisation needed.
func startCluster(t *testing.T, cfg transport.Config, ids []string, down map[string]bool) *cluster {
	t.Helper()

	addrs := make(map[string]string, len(ids))
	listeners := make(map[string]net.Listener, len(ids))
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { lis.Close() })
		addrs[id] = lis.Addr().String()
		listeners[id] = lis
	}

	r := ring.New(ids)
	stores := make(map[string]*store.Memory, len(ids))
	for _, id := range ids {
		st := store.NewMemory()
		stores[id] = st
		if down[id] {
			listeners[id].Close() // registered, but refuses connections
			continue
		}
		srv := transport.NewServer(id, addrs, r, st, cfg)
		go http.Serve(listeners[id], srv.Handler())
	}

	return &cluster{addrs: addrs, stores: stores, ring: r}
}

func (c *cluster) put(t *testing.T, viaNode, key, value string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut,
		"http://"+c.addrs[viaNode]+"/kv/"+key,
		strings.NewReader(fmt.Sprintf(`{"value":%q}`, value)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func (c *cluster) get(t *testing.T, viaNode, key string) *http.Response {
	t.Helper()
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + c.addrs[viaNode] + "/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// keyOwnedBy finds a key whose coordinator (first preference-list entry)
// is nodeID.
func keyOwnedBy(r *ring.Ring, nodeID string) string {
	for i := 0; ; i++ {
		key := fmt.Sprintf("key-%d", i)
		if r.Owner(key) == nodeID {
			return key
		}
	}
}

func TestWrite_ReplicatesToAllPreferredNodes(t *testing.T) {
	ids := []string{"A", "B", "C"}
	c := startCluster(t, transport.Config{N: 3, R: 2, W: 2}, ids, nil)

	key := "cart123"
	resp := c.put(t, "A", key, "laptop")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}

	// With N=3 on a 3-node cluster, every node should end up holding it.
	for _, id := range ids {
		versions, _ := c.stores[id].Get(key)
		if len(versions) != 1 || string(versions[0].Value) != "laptop" {
			t.Errorf("node %s holds %v, want one version %q", id, versions, "laptop")
		}
	}
}

func TestWrite_SucceedsWithOneReplicaDown(t *testing.T) {
	ids := []string{"A", "B", "C"}
	r := ring.New(ids)
	key := "cart123"
	pref := r.PreferenceList(key, 3)

	// Take down the *last* preferred replica: the coordinator survives, so
	// W=2 is still reachable via the coordinator plus one other.
	c := startCluster(t, transport.Config{N: 3, R: 2, W: 2}, ids, map[string]bool{pref[2]: true})

	resp := c.put(t, pref[0], key, "laptop")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200 (W=2 should tolerate one node down)", resp.StatusCode)
	}

	var body struct{ Replicas int }
	json.NewDecoder(resp.Body).Decode(&body)
	if body.Replicas != 2 {
		t.Errorf("replicas = %d, want 2", body.Replicas)
	}
}

func TestWrite_FailsWhenQuorumUnreachable(t *testing.T) {
	ids := []string{"A", "B", "C"}
	r := ring.New(ids)
	key := "cart123"
	pref := r.PreferenceList(key, 3)

	// W=3 requires every replica, so one node down must fail the write.
	c := startCluster(t, transport.Config{N: 3, R: 2, W: 3}, ids, map[string]bool{pref[2]: true})

	resp := c.put(t, pref[0], key, "laptop")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("PUT status = %d, want 503 (W=3 cannot be met with one node down)", resp.StatusCode)
	}
}

func TestRead_ReturnsValueViaAnyNode(t *testing.T) {
	ids := []string{"A", "B", "C"}
	c := startCluster(t, transport.Config{N: 3, R: 2, W: 2}, ids, nil)

	key := "cart123"
	c.put(t, "A", key, "laptop").Body.Close()

	// Every node is a valid entry point, coordinator or not.
	for _, via := range ids {
		resp := c.get(t, via, key)
		var body struct {
			Versions []struct{ Value string }
			Replicas int
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET via %s: status %d, want 200", via, resp.StatusCode)
		}
		if len(body.Versions) != 1 || body.Versions[0].Value != "laptop" {
			t.Errorf("GET via %s returned %+v, want one version %q", via, body.Versions, "laptop")
		}
	}
}

func TestRead_FailsWhenQuorumUnreachable(t *testing.T) {
	ids := []string{"A", "B", "C"}
	r := ring.New(ids)
	key := "cart123"
	pref := r.PreferenceList(key, 3)

	// R=3 with one replica down cannot be satisfied.
	c := startCluster(t, transport.Config{N: 3, R: 3, W: 2}, ids, map[string]bool{pref[2]: true})

	resp := c.get(t, pref[0], key)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET status = %d, want 503", resp.StatusCode)
	}
}

func TestRead_ReconcilesStaleReplica(t *testing.T) {
	ids := []string{"A", "B", "C"}
	c := startCluster(t, transport.Config{N: 3, R: 2, W: 2}, ids, nil)

	key := "cart123"
	c.put(t, "A", key, "v1").Body.Close()
	c.put(t, "A", key, "v2").Body.Close()

	// Plant an older version directly on one replica, as if it had missed
	// the second write. The read must discard it rather than report a
	// conflict, since it is an ancestor of what the others hold.
	stale := store.VersionedValue{Value: []byte("stale"), Clock: nil}
	if err := c.stores["B"].Put(key, stale); err != nil {
		t.Fatal(err)
	}

	resp := c.get(t, c.ring.Owner(key), key)
	defer resp.Body.Close()
	var body struct {
		Versions []struct{ Value string }
	}
	json.NewDecoder(resp.Body).Decode(&body)

	if len(body.Versions) != 1 || body.Versions[0].Value != "v2" {
		t.Fatalf("got %+v, want a single version %q", body.Versions, "v2")
	}
}

func TestForward_FailsWhenCoordinatorUnreachable(t *testing.T) {
	ids := []string{"A", "B"}
	r := ring.New(ids)
	key := keyOwnedBy(r, "B")

	// B coordinates this key but is down; A can only report a bad gateway.
	c := startCluster(t, transport.Config{N: 2, R: 1, W: 1}, ids, map[string]bool{"B": true})

	resp := c.get(t, "A", key)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     transport.Config
		wantErr bool
	}{
		{"default is valid", transport.DefaultConfig(), false},
		{"R may equal N", transport.Config{N: 3, R: 3, W: 1}, false},
		{"N below one", transport.Config{N: 0, R: 1, W: 1}, true},
		{"R above N", transport.Config{N: 3, R: 4, W: 2}, true},
		{"W below one", transport.Config{N: 3, R: 2, W: 0}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.cfg.Validate(); (err != nil) != c.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, c.wantErr)
			}
		})
	}
}
