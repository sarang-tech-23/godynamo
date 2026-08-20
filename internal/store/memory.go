package store

import (
	"sync"

	"godynamo/internal/version"
)

// Memory is an in-memory Store guarded by a mutex.
type Memory struct {
	mu   sync.RWMutex
	data map[string][]VersionedValue
}

func NewMemory() *Memory {
	return &Memory{data: make(map[string][]VersionedValue)}
}

// Get returns every causally-divergent version currently held for key. The
// returned slice is a copy; callers may not mutate the store through it.
func (m *Memory) Get(key string) ([]VersionedValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	existing := m.data[key]
	out := make([]VersionedValue, len(existing))
	copy(out, existing)
	return out, nil
}

// Put merges v into the versions stored for key.
//
//   - if some stored version already equals or descends from v, v is stale
//     (or a duplicate) and is dropped; nothing changes.
//   - otherwise v is saved, and any stored version that v descends from is
//     pruned. v is saved before pruning, so a version this call accepted is
//     never lost if it were interrupted between the two steps.
//   - versions concurrent with v are left untouched and now sit alongside
//     it as siblings.
func (m *Memory) Put(key string, v VersionedValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing := m.data[key]
	for _, e := range existing {
		if version.Equal(e.Clock, v.Clock) || version.Descends(e.Clock, v.Clock) {
			return nil
		}
	}

	combined := append(existing, v)

	kept := make([]VersionedValue, 0, len(combined))
	for _, e := range combined {
		if !version.Descends(v.Clock, e.Clock) {
			kept = append(kept, e)
		}
	}
	m.data[key] = kept
	return nil
}

func (m *Memory) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}
