// Command node runs a single Dynamo-style node: it serves the external
// PUT/GET API and forwards requests to whichever peer coordinates a given
// key.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"godynamo/internal/ring"
	"godynamo/internal/store"
	"godynamo/internal/transport"
)

func main() {
	var (
		id    = flag.String("id", "", "this node's ID (required)")
		peers = flag.String("peers", "", "comma-separated id=host:port pairs for every node in the cluster, e.g. A=localhost:8001,B=localhost:8002 (required; must be identical across every node's process)")
	)
	flag.Parse()

	if *id == "" || *peers == "" {
		log.Fatal("--id and --peers are required")
	}

	addrs, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("invalid --peers: %v", err)
	}

	addr, ok := addrs[*id]
	if !ok {
		log.Fatalf("--id %q not present in --peers", *id)
	}

	members := make([]string, 0, len(addrs))
	for nodeID := range addrs {
		members = append(members, nodeID)
	}

	r := ring.New(members)
	s := store.NewMemory()
	srv := transport.NewServer(*id, addrs, r, s)

	log.Printf("node %s listening on %s (peers: %v)", *id, addr, addrs)
	log.Fatal(http.ListenAndServe(addr, srv.Handler()))
}

// parsePeers parses "A=host:port,B=host:port,..." into a node ID -> address map.
func parsePeers(s string) (map[string]string, error) {
	addrs := make(map[string]string)
	for _, pair := range strings.Split(s, ",") {
		id, addr, found := strings.Cut(pair, "=")
		if !found {
			return nil, fmt.Errorf("expected id=host:port, got %q", pair)
		}
		addrs[id] = addr
	}
	return addrs, nil
}
