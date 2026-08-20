// Package version implements vector clocks for tracking causality between
// versions of a key, as described in section 4.4 of the Dynamo paper.
package version

// VectorClock maps a coordinating node's ID to the number of writes it has
// coordinated for a given key.
type VectorClock map[string]uint64

// Increment returns a new VectorClock with node's counter incremented by one.
// vc may be nil (an unseen key); the result is never nil. vc is not mutated.
func Increment(vc VectorClock, node string) VectorClock {
	next := make(VectorClock, len(vc)+1)
	for k, v := range vc {
		next[k] = v
	}
	next[node]++
	return next
}

// Equal reports whether a and b have identical counters for every node.
func Equal(a, b VectorClock) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// descendsOrEqual reports whether a's counters are pointwise >= b's,
// treating an absent entry as 0.
func descendsOrEqual(a, b VectorClock) bool {
	for k, v := range b {
		if a[k] < v { // absent keys in go maps return zero values for they types, so for uinit64, this is 0. for string it would be ""
			return false
		}
	}
	return true
}

// Descends reports whether a is a strict descendant of b: a happened after
// b, i.e. a's counters are pointwise >= b's with at least one strictly
// greater.
func Descends(a, b VectorClock) bool {
	return descendsOrEqual(a, b) && !Equal(a, b)
}

// Concurrent reports whether a and b are causally unrelated: neither
// descends from the other.
func Concurrent(a, b VectorClock) bool {
	return !descendsOrEqual(a, b) && !descendsOrEqual(b, a)
}

// Merge returns the pointwise maximum of a and b's counters. The result
// descends from (or equals) both a and b, which is what lets a client's
// reconciled write causally supersede every sibling it was reconciled from.
func Merge(a, b VectorClock) VectorClock {
	merged := make(VectorClock, len(a)+len(b))
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		if v > merged[k] {
			merged[k] = v
		}
	}
	return merged
}
