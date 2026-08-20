// Package ring implements a consistent-hash ring for mapping keys to the
// physical nodes responsible for them.
package ring

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
)

// VNodesPerNode is how many virtual tokens each physical node is assigned.
// More tokens per node means finer-grained, more evenly spread load and
// less disruption when membership changes, at the cost of a bigger token
// table to walk.
const VNodesPerNode = 64

// Token is one position a physical node occupies on the ring.
type Token struct {
	Position uint64
	NodeID   string
}

// Ring is a consistent-hash ring built from a static set of member nodes.
// Membership is fixed at construction time -- there is no gossip protocol
// to learn about nodes joining or leaving, so every process is expected to
// build its Ring from the same static member list.
type Ring struct {
	tokens []Token // sorted by Position ascending
}

// New builds a ring from a static list of physical node IDs. Each node's
// tokens are derived deterministically from its ID, so any process given
// the same member list constructs an identical ring without needing to
// learn or persist token assignments from anywhere else.
func New(members []string) *Ring {
	tokens := make([]Token, 0, len(members)*VNodesPerNode)
	for _, node := range members {
		for i := 0; i < VNodesPerNode; i++ {
			tokens = append(tokens, Token{
				Position: hashPosition(fmt.Sprintf("%s#%d", node, i)),
				NodeID:   node,
			})
		}
	}
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i].Position < tokens[j].Position
	})
	return &Ring{tokens: tokens}
}

// hashPosition maps s onto the ring's position space using the first 8
// bytes of its MD5 digest.
func hashPosition(s string) uint64 {
	sum := md5.Sum([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

// Owner returns the physical node responsible for key.
func (r *Ring) Owner(key string) string {
	list := r.PreferenceList(key, 1)
	if len(list) == 0 {
		return ""
	}
	return list[0]
}

// PreferenceList returns up to n distinct physical nodes for key, in ring
// order starting from key's position. Callers that also need sloppy-quorum
// fallbacks should ask for more than the replication factor N; the first N
// entries are the preferred replicas and the remainder is the fallback
// pool.
func (r *Ring) PreferenceList(key string, n int) []string {
	return r.preferenceListFrom(hashPosition(key), n)
}

// preferenceListFrom walks the ring clockwise from pos, split out from
// PreferenceList so the walk/wraparound logic can be tested against
// synthetic positions without depending on what a real key happens to
// hash to.
func (r *Ring) preferenceListFrom(pos uint64, n int) []string {
	if len(r.tokens) == 0 || n <= 0 {
		return nil
	}

	start := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].Position >= pos
	})

	seen := make(map[string]bool, n)
	list := make([]string, 0, n)
	for i := 0; i < len(r.tokens) && len(list) < n; i++ {
		tok := r.tokens[(start+i)%len(r.tokens)]
		if seen[tok.NodeID] {
			continue
		}
		seen[tok.NodeID] = true
		list = append(list, tok.NodeID)
	}
	return list
}
