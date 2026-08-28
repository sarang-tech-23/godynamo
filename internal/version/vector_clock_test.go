package version

import "testing"

func TestIncrement(t *testing.T) {
	t.Run("first write on an unseen key", func(t *testing.T) {
		got := Increment(nil, "A")
		want := VectorClock{"A": 1}
		if !Equal(got, want) {
			t.Fatalf("Increment(nil, A) = %v, want %v", got, want)
		}
	})

	t.Run("increments an existing entry", func(t *testing.T) {
		vc := VectorClock{"A": 1, "B": 3}
		got := Increment(vc, "A")
		want := VectorClock{"A": 2, "B": 3}
		if !Equal(got, want) {
			t.Fatalf("Increment(%v, A) = %v, want %v", vc, got, want)
		}
	})

	t.Run("adds a new entry for an unseen node", func(t *testing.T) {
		vc := VectorClock{"A": 1}
		got := Increment(vc, "B")
		want := VectorClock{"A": 1, "B": 1}
		if !Equal(got, want) {
			t.Fatalf("Increment(%v, B) = %v, want %v", vc, got, want)
		}
	})

	t.Run("does not mutate the input", func(t *testing.T) {
		vc := VectorClock{"A": 1}
		_ = Increment(vc, "A")
		if vc["A"] != 1 {
			t.Fatalf("Increment mutated its input: got %v", vc)
		}
	})
}

func TestEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b VectorClock
		want bool
	}{
		{"both empty", VectorClock{}, VectorClock{}, true},
		{"nil and empty", nil, VectorClock{}, true},
		{"identical", VectorClock{"A": 1, "B": 2}, VectorClock{"A": 1, "B": 2}, true},
		{"different counter", VectorClock{"A": 1}, VectorClock{"A": 2}, false},
		{"different keys same length", VectorClock{"A": 1}, VectorClock{"B": 1}, false},
		{"different length", VectorClock{"A": 1}, VectorClock{"A": 1, "B": 1}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Equal(c.a, c.b); got != c.want {
				t.Fatalf("Equal(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestDescends(t *testing.T) {
	cases := []struct {
		name string
		a, b VectorClock
		want bool
	}{
		{
			name: "[(A,2)] descends [(A,1)]",
			a:    VectorClock{"A": 2},
			b:    VectorClock{"A": 1},
			want: true,
		},
		{
			name: "[(A,1)] does not descend [(A,2)]",
			a:    VectorClock{"A": 1},
			b:    VectorClock{"A": 2},
			want: false,
		},
		{
			name: "[(A,2),(B,1)] descends [(A,2)]",
			a:    VectorClock{"A": 2, "B": 1},
			b:    VectorClock{"A": 2},
			want: true,
		},
		{
			name: "[(A,2)] does not descend [(A,2),(B,1)]",
			a:    VectorClock{"A": 2},
			b:    VectorClock{"A": 2, "B": 1},
			want: false,
		},
		{
			name: "equal clocks do not descend each other",
			a:    VectorClock{"A": 2},
			b:    VectorClock{"A": 2},
			want: false,
		},
		{
			name: "concurrent clocks do not descend each other",
			a:    VectorClock{"A": 2, "B": 1},
			b:    VectorClock{"A": 2, "C": 1},
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Descends(c.a, c.b); got != c.want {
				t.Fatalf("Descends(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestConcurrent(t *testing.T) {
	cases := []struct {
		name string
		a, b VectorClock
		want bool
	}{
		{
			name: "[(A,2),(B,1)] vs [(A,2),(C,1)] are concurrent",
			a:    VectorClock{"A": 2, "B": 1},
			b:    VectorClock{"A": 2, "C": 1},
			want: true,
		},
		{
			name: "ancestor/descendant pair is not concurrent",
			a:    VectorClock{"A": 1},
			b:    VectorClock{"A": 2},
			want: false,
		},
		{
			name: "equal clocks are not concurrent",
			a:    VectorClock{"A": 1},
			b:    VectorClock{"A": 1},
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Concurrent(c.a, c.b); got != c.want {
				t.Fatalf("Concurrent(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
			// Concurrency must be symmetric.
			if got := Concurrent(c.b, c.a); got != c.want {
				t.Fatalf("Concurrent(%v, %v) = %v, want %v", c.b, c.a, got, c.want)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	t.Run("takes the pointwise max", func(t *testing.T) {
		a := VectorClock{"A": 2, "B": 1}
		b := VectorClock{"A": 1, "C": 1}
		got := Merge(a, b)
		want := VectorClock{"A": 2, "B": 1, "C": 1}
		if !Equal(got, want) {
			t.Fatalf("Merge(%v, %v) = %v, want %v", a, b, got, want)
		}
	})

	t.Run("merge of concurrent siblings descends from both", func(t *testing.T) {
		a := VectorClock{"A": 2, "B": 1}
		b := VectorClock{"A": 2, "C": 1}
		if !Concurrent(a, b) {
			t.Fatalf("test setup invalid: %v and %v are not concurrent", a, b)
		}
		merged := Merge(a, b)
		if !Descends(merged, a) {
			t.Fatalf("Merge(%v, %v) = %v does not descend from %v", a, b, merged, a)
		}
		if !Descends(merged, b) {
			t.Fatalf("Merge(%v, %v) = %v does not descend from %v", a, b, merged, b)
		}
	})
}
