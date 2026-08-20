package ring

import (
	"fmt"
	"testing"
)

func TestOwner_Deterministic(t *testing.T) {
	members := []string{"A", "B", "C", "D"}
	r1 := New(members)
	r2 := New(members)

	for _, key := range []string{"cart123", "user42", "order-99"} {
		o1, o2 := r1.Owner(key), r2.Owner(key)
		if o1 != o2 {
			t.Fatalf("Owner(%q) not deterministic across identically-built rings: %q vs %q", key, o1, o2)
		}
	}
}

func TestPreferenceList_DistinctPhysicalNodes(t *testing.T) {
	r := New([]string{"A", "B", "C", "D"})

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		list := r.PreferenceList(key, 3)
		if len(list) != 3 {
			t.Fatalf("PreferenceList(%q, 3) returned %d nodes, want 3: %v", key, len(list), list)
		}
		seen := make(map[string]bool, 3)
		for _, node := range list {
			if seen[node] {
				t.Fatalf("PreferenceList(%q, 3) = %v contains duplicate node %q", key, list, node)
			}
			seen[node] = true
		}
	}
}

func TestPreferenceList_CappedAtMemberCount(t *testing.T) {
	r := New([]string{"A", "B"})
	list := r.PreferenceList("any-key", 5)
	if len(list) != 2 {
		t.Fatalf("PreferenceList with 2 members and n=5 returned %d nodes, want 2: %v", len(list), list)
	}
}

func TestOwner_EmptyRing(t *testing.T) {
	r := New(nil)
	if got := r.Owner("any-key"); got != "" {
		t.Fatalf("Owner on empty ring = %q, want empty string", got)
	}
}

// preferenceListFrom is tested directly against a synthetic ring so the
// clockwise-walk and wraparound logic can be checked against known
// positions, independent of what any real key happens to hash to.
func TestPreferenceListFrom_WalksClockwiseAndWraps(t *testing.T) {
	r := &Ring{
		tokens: []Token{
			{Position: 10, NodeID: "A"},
			{Position: 20, NodeID: "B"},
			{Position: 30, NodeID: "C"},
		},
	}

	cases := []struct {
		name string
		pos  uint64
		n    int
		want []string
	}{
		{"lands exactly on a token", 20, 2, []string{"B", "C"}},
		{"lands between tokens", 15, 2, []string{"B", "C"}},
		{"before the first token", 0, 3, []string{"A", "B", "C"}},
		{"past the last token wraps to the first", 31, 2, []string{"A", "B"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := r.preferenceListFrom(c.pos, c.n)
			if len(got) != len(c.want) {
				t.Fatalf("preferenceListFrom(%d, %d) = %v, want %v", c.pos, c.n, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("preferenceListFrom(%d, %d) = %v, want %v", c.pos, c.n, got, c.want)
				}
			}
		})
	}
}

func TestPreferenceListFrom_SkipsRepeatedPhysicalNode(t *testing.T) {
	// A owns two adjacent tokens; the walk must not count A twice.
	r := &Ring{
		tokens: []Token{
			{Position: 10, NodeID: "A"},
			{Position: 15, NodeID: "A"},
			{Position: 20, NodeID: "B"},
		},
	}
	got := r.preferenceListFrom(0, 2)
	want := []string{"A", "B"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("preferenceListFrom(0, 2) = %v, want %v", got, want)
	}
}

// TestAddingNodeMovesOnlyPartOfKeyspace is the property that motivates
// virtual nodes over a naive hashing scheme: adding one node to an
// n-member ring should reassign roughly 1/(n+1) of keys, not all of them.
func TestAddingNodeMovesOnlyPartOfKeyspace(t *testing.T) {
	before := New([]string{"A", "B", "C"})
	after := New([]string{"A", "B", "C", "D"})

	const numKeys = 2000
	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		if before.Owner(key) != after.Owner(key) {
			moved++
		}
	}

	fraction := float64(moved) / float64(numKeys)
	// Expected ~1/4 of keys move (new node claims ~1/(n+1) of the ring).
	// Allow generous slack for hash variance, but a naive scheme where
	// nearly every key moves should clearly fail this bound.
	if fraction <= 0 {
		t.Fatalf("adding a node moved no keys at all (moved=%d)", moved)
	}
	if fraction > 0.5 {
		t.Fatalf("adding a node moved %.1f%% of keys, want well under 50%%", fraction*100)
	}
	t.Logf("adding a 4th node moved %.1f%% of %d keys", fraction*100, numKeys)
}
