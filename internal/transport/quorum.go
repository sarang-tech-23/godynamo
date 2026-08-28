package transport

import (
	"context"

	"godynamo/internal/store"
)

// replicate sends v to every replica concurrently and returns how many
// acknowledged. It returns as soon as `need` acknowledgements have arrived
// rather than waiting for the slowest replica -- that early return is the
// entire point of a write quorum.
func (s *Server) replicate(ctx context.Context, key string, v store.VersionedValue, replicas []string, need int) int {
	if len(replicas) == 0 {
		return 0
	}

	// Buffered to the full fan-out width so that a straggler can always
	// deliver its result and exit, even long after we have stopped
	// reading. With an unbuffered channel those goroutines would park on
	// the send forever: a goroutine leak on every slow replica.
	results := make(chan error, len(replicas))
	for _, node := range replicas {
		go func() {
			results <- s.replicaPut(ctx, node, key, v)
		}()
	}

	acks := 0
	for range replicas {
		select {
		case err := <-results:
			if err == nil {
				acks++
				if acks >= need {
					return acks
				}
			}
		case <-ctx.Done():
			// Out of time. Whatever acks we have is what the caller gets;
			// the in-flight writes are cancelled with the context.
			return acks
		}
	}
	return acks
}

// gather reads key from every replica concurrently, returning every
// version seen along with how many replicas answered. Like replicate, it
// stops waiting once `need` replicas have responded.
//
// Note it fans out to every replica but only waits for `need` of them:
// asking only R nodes would make one slow node fail the read, and would
// hide the stale replicas that read repair needs to see.
func (s *Server) gather(ctx context.Context, key string, replicas []string, need int) ([]store.VersionedValue, int) {
	if len(replicas) == 0 {
		return nil, 0
	}

	type result struct {
		versions []store.VersionedValue
		err      error
	}
	results := make(chan result, len(replicas))
	for _, node := range replicas {
		go func() {
			versions, err := s.replicaGet(ctx, node, key)
			results <- result{versions: versions, err: err}
		}()
	}

	var collected []store.VersionedValue
	responded := 0
	for range replicas {
		select {
		case res := <-results:
			if res.err == nil {
				responded++
				collected = append(collected, res.versions...)
				if responded >= need {
					return collected, responded
				}
			}
		case <-ctx.Done():
			return collected, responded
		}
	}
	return collected, responded
}
