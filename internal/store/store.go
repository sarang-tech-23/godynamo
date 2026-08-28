// Package store implements the local per-node storage of versioned values.
package store

import (
	"fmt"
	"godynamo/internal/version"
)

// VersionedValue is a value together with the vector clock describing its
// causal history.
type VersionedValue struct {
	Value []byte
	Clock version.VectorClock
}

// Store is the local storage abstraction for a single node. Put applies the
// merge rule from the Dynamo paper: an incoming version that is causally
// dominated by a version already stored is dropped; a version it dominates
// is pruned; concurrent versions are kept side by side as siblings.
type Store interface {
	Get(key string) ([]VersionedValue, error)
	Put(key string, v VersionedValue) error
	Delete(key string) error
}

// CoordinatorPut builds a new VersionedValue by incrementing context (the
// vector clock the client observed on its last read of key, or nil for a
// key it has never read) with nodeID, then writes it to s using the same
// merge rule any replica applies. It returns the version that was written
// so the caller can forward the identical bytes to replica nodes.
func CoordinatorPut(s Store, key string, value []byte, context version.VectorClock, nodeID string) (VersionedValue, error) {
	existing, err := s.Get(key)
	if err != nil {
		return VersionedValue{}, err
	}

	clock := version.Increment(context, nodeID)

	// The coordinator's own counter must also advance past anything it has
	// already issued for this key. Without this, a client writing with no
	// context (or a stale one) would regenerate a clock identical to a
	// stored version, and Put would discard the write as a duplicate.
	//
	// Note the consequence: two concurrent writes coordinated by the *same*
	// node are serialised rather than kept as siblings. That matches the
	// paper -- siblings arise when *different* nodes coordinate writes to
	// the same key, which is what happens under partition or failover.
	for _, e := range existing {
		if e.Clock[nodeID] >= clock[nodeID] {
			clock[nodeID] = e.Clock[nodeID] + 1
		}
	}

	v := VersionedValue{Value: value, Clock: clock}
	if err := s.Put(key, v); err != nil {
		return VersionedValue{}, err
	}
	fmt.Println("coordinator put for key", key, "with clock", v.Clock)
	return v, nil
}
