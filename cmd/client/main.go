// Command client is a minimal CLI for sending a single GET or PUT to one
// node's external API -- useful for poking at a running cluster by hand.
// It does not track vector-clock context between calls; each invocation is
// a fresh, unrelated request.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func main() {
	var (
		port  = flag.Int("port", 0, "port of the node to talk to (required)")
		op    = flag.String("op", "", "operation: get or put (required)")
		key   = flag.String("key", "", "key (required)")
		value = flag.String("value", "", "value (required for put)")
	)
	flag.Parse()

	if *port == 0 || *key == "" {
		log.Fatal("--port and --key are required")
	}

	url := fmt.Sprintf("http://localhost:%d/kv/%s", *port, *key)

	var (
		resp *http.Response
		err  error
	)
	switch strings.ToLower(*op) {
	case "get":
		resp, err = http.Get(url)

	case "put":
		if *value == "" {
			log.Fatal("--value is required for put")
		}
		body, marshalErr := json.Marshal(struct {
			Value string `json:"value"`
		}{Value: *value})
		if marshalErr != nil {
			log.Fatal(marshalErr)
		}
		req, reqErr := http.NewRequest(http.MethodPut, url, strings.NewReader(string(body)))
		if reqErr != nil {
			log.Fatal(reqErr)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)

	default:
		log.Fatalf("unknown --op %q, want get or put", *op)
	}

	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n%s\n", resp.Status, respBody)
}
