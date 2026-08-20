package store

import (
	"testing"

	"godynamo/internal/version"
)

func TestMemoryPut_MergeRule(t *testing.T) {
	t.Run("first write for a key is simply stored", func(t *testing.T) {
		m := NewMemory()
		v := VersionedValue{Value: []byte("v1"), Clock: version.VectorClock{"A": 1}}
		if err := m.Put("k", v); err != nil {
			t.Fatal(err)
		}
		assertVersions(t, m, "k", v)
	})

	t.Run("descendant replaces its ancestor", func(t *testing.T) {
		m := NewMemory()
		v1 := VersionedValue{Value: []byte("v1"), Clock: version.VectorClock{"A": 1}}
		v2 := VersionedValue{Value: []byte("v2"), Clock: version.VectorClock{"A": 2}}
		put(t, m, "k", v1)
		put(t, m, "k", v2)
		assertVersions(t, m, "k", v2)
	})

	t.Run("stale ancestor arriving late is dropped", func(t *testing.T) {
		m := NewMemory()
		v1 := VersionedValue{Value: []byte("v1"), Clock: version.VectorClock{"A": 1}}
		v2 := VersionedValue{Value: []byte("v2"), Clock: version.VectorClock{"A": 2}}
		put(t, m, "k", v2)
		put(t, m, "k", v1) // arrives after v2; should be dropped
		assertVersions(t, m, "k", v2)
	})

	t.Run("duplicate write is idempotent", func(t *testing.T) {
		m := NewMemory()
		v := VersionedValue{Value: []byte("v1"), Clock: version.VectorClock{"A": 1}}
		put(t, m, "k", v)
		put(t, m, "k", v)
		assertVersions(t, m, "k", v)
	})

	t.Run("concurrent versions are kept as siblings", func(t *testing.T) {
		m := NewMemory()
		v1 := VersionedValue{Value: []byte("v1"), Clock: version.VectorClock{"A": 1}}
		v2 := VersionedValue{Value: []byte("v2"), Clock: version.VectorClock{"B": 1}}
		put(t, m, "k", v1)
		put(t, m, "k", v2)
		assertVersions(t, m, "k", v1, v2)
	})

	t.Run("descendant of one sibling prunes only that sibling", func(t *testing.T) {
		m := NewMemory()
		v1 := VersionedValue{Value: []byte("v1"), Clock: version.VectorClock{"A": 1}}
		v2 := VersionedValue{Value: []byte("v2"), Clock: version.VectorClock{"B": 1}}
		v3 := VersionedValue{Value: []byte("v3"), Clock: version.VectorClock{"A": 2}} // descends v1 only
		put(t, m, "k", v1)
		put(t, m, "k", v2)
		put(t, m, "k", v3)
		assertVersions(t, m, "k", v2, v3)
	})

	t.Run("reconciled merge write collapses all siblings", func(t *testing.T) {
		m := NewMemory()
		v1 := VersionedValue{Value: []byte("v1"), Clock: version.VectorClock{"A": 1}}
		v2 := VersionedValue{Value: []byte("v2"), Clock: version.VectorClock{"B": 1}}
		put(t, m, "k", v1)
		put(t, m, "k", v2)

		merged := VersionedValue{
			Value: []byte("reconciled"),
			Clock: version.Merge(v1.Clock, v2.Clock),
		}
		put(t, m, "k", merged)
		assertVersions(t, m, "k", merged)
	})
}

func TestCoordinatorPut(t *testing.T) {
	t.Run("first write for a key needs no context", func(t *testing.T) {
		m := NewMemory()
		v, err := CoordinatorPut(m, "k", []byte("v1"), nil, "A")
		if err != nil {
			t.Fatal(err)
		}
		want := version.VectorClock{"A": 1}
		if !version.Equal(v.Clock, want) {
			t.Fatalf("clock = %v, want %v", v.Clock, want)
		}
		assertVersions(t, m, "k", v)
	})

	t.Run("write with stale context creates a sibling instead of clobbering", func(t *testing.T) {
		m := NewMemory()
		v1, err := CoordinatorPut(m, "k", []byte("v1"), nil, "A")
		if err != nil {
			t.Fatal(err)
		}
		// A second client read nothing (nil context) and writes concurrently.
		v2, err := CoordinatorPut(m, "k", []byte("v2"), nil, "B")
		if err != nil {
			t.Fatal(err)
		}
		assertVersions(t, m, "k", v1, v2)
	})

	t.Run("write with up-to-date context supersedes the prior version", func(t *testing.T) {
		m := NewMemory()
		v1, err := CoordinatorPut(m, "k", []byte("v1"), nil, "A")
		if err != nil {
			t.Fatal(err)
		}
		v2, err := CoordinatorPut(m, "k", []byte("v2"), v1.Clock, "A")
		if err != nil {
			t.Fatal(err)
		}
		assertVersions(t, m, "k", v2)
	})
}

func put(t *testing.T, s Store, key string, v VersionedValue) {
	t.Helper()
	if err := s.Put(key, v); err != nil {
		t.Fatalf("Put(%q, %v) failed: %v", key, v, err)
	}
}

// assertVersions checks that Get(key) returns exactly the given versions,
// ignoring order.
func assertVersions(t *testing.T, s Store, key string, want ...VersionedValue) {
	t.Helper()
	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", key, err)
	}
	if len(got) != len(want) {
		t.Fatalf("Get(%q) = %v, want %v", key, got, want)
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			if string(g.Value) == string(w.Value) && version.Equal(g.Clock, w.Clock) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Get(%q) = %v, missing want %v", key, got, w)
		}
	}
}
