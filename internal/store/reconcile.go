package store

import "godynamo/internal/version"

// Reconcile reduces versions gathered from several replicas to the set
// that is still causally current: any version some other version descends
// from is dropped, and identical versions returned by different replicas
// are collapsed into one.
//
// What survives is an antichain under the happened-before ordering --
// versions that are mutually concurrent, i.e. genuine conflicting siblings
// the client is expected to resolve. In the ordinary case exactly one
// version survives.
func Reconcile(versions []VersionedValue) []VersionedValue {
	kept := make([]VersionedValue, 0, len(versions))
	for i, v := range versions {
		dominated := false
		for j, other := range versions {
			if i != j && version.Descends(other.Clock, v.Clock) {
				dominated = true
				break
			}
		}
		if dominated {
			continue
		}

		// Several replicas holding the same version is the normal case,
		// not a conflict; return it once.
		duplicate := false
		for _, k := range kept {
			if version.Equal(k.Clock, v.Clock) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			kept = append(kept, v)
		}
	}
	return kept
}

// ContextOf returns the merged clock of every given version. A client that
// echoes this back on its next write produces a version that descends from
// all of them, which is what collapses siblings after reconciliation.
func ContextOf(versions []VersionedValue) version.VectorClock {
	merged := version.VectorClock{}
	for _, v := range versions {
		merged = version.Merge(merged, v.Clock)
	}
	return merged
}
