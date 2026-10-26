package vv

import (
	"math/rand/v2"
	"testing"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b Vector
		want Order
	}{
		{"both empty", Vector{}, Vector{}, Equal},
		{"nil and empty", nil, Vector{}, Equal},
		{"same", Vector{"a": 1, "b": 2}, Vector{"a": 1, "b": 2}, Equal},
		{"zero entry equals missing", Vector{"a": 1, "b": 0}, Vector{"a": 1}, Equal},
		{"empty before non-empty", Vector{}, Vector{"a": 1}, Before},
		{"non-empty after empty", Vector{"a": 1}, Vector{}, After},
		{"one counter higher", Vector{"a": 2, "b": 1}, Vector{"a": 1, "b": 1}, After},
		{"one counter lower", Vector{"a": 1, "b": 1}, Vector{"a": 1, "b": 2}, Before},
		{"extra node", Vector{"a": 1, "c": 1}, Vector{"a": 1}, After},
		{"missing node", Vector{"a": 1}, Vector{"a": 1, "c": 1}, Before},
		{"concurrent same keys", Vector{"a": 2, "b": 1}, Vector{"a": 1, "b": 2}, Concurrent},
		{"concurrent disjoint keys", Vector{"a": 1}, Vector{"b": 1}, Concurrent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Compare(tt.a, tt.b); got != tt.want {
				t.Errorf("Compare(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	got := Merge(Vector{"a": 3, "b": 1}, Vector{"b": 2, "c": 1})
	want := Vector{"a": 3, "b": 2, "c": 1}
	if Compare(got, want) != Equal {
		t.Errorf("Merge = %v, want %v", got, want)
	}
}

func TestMergeDoesNotModifyInputs(t *testing.T) {
	a := Vector{"a": 1}
	b := Vector{"b": 1}
	Merge(a, b)
	if len(a) != 1 || len(b) != 1 {
		t.Errorf("inputs changed: %v %v", a, b)
	}
}

func TestBump(t *testing.T) {
	v := Vector{"a": 1}
	got := Bump(v, "b")
	if got["a"] != 1 || got["b"] != 1 {
		t.Errorf("Bump = %v", got)
	}
	if _, ok := v["b"]; ok {
		t.Errorf("Bump modified input: %v", v)
	}
	if Bump(nil, "a")["a"] != 1 {
		t.Errorf("Bump(nil) failed")
	}
}

func TestEncodeIsCanonical(t *testing.T) {
	a := Vector{}
	a["zeta"] = 1
	a["alpha"] = 2
	a["mid"] = 3
	b := Vector{}
	b["mid"] = 3
	b["alpha"] = 2
	b["zeta"] = 1
	b["unused"] = 0
	want := `{"alpha":2,"mid":3,"zeta":1}`
	if Encode(a) != want || Encode(b) != want {
		t.Errorf("Encode = %s and %s, want %s", Encode(a), Encode(b), want)
	}
	if Encode(nil) != "{}" {
		t.Errorf("Encode(nil) = %s", Encode(nil))
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	v := Vector{"a": 5, "b": 7}
	got, err := Decode(Encode(v))
	if err != nil {
		t.Fatal(err)
	}
	if Compare(got, v) != Equal {
		t.Errorf("round trip = %v, want %v", got, v)
	}
	if _, err := Decode("not json"); err == nil {
		t.Error("expected error for invalid input")
	}
}

func randomVector(rng *rand.Rand) Vector {
	v := Vector{}
	for _, id := range []string{"a", "b", "c"} {
		if n := rng.IntN(4); n > 0 {
			v[id] = uint64(n)
		}
	}
	return v
}

func mirror(o Order) Order {
	switch o {
	case Before:
		return After
	case After:
		return Before
	default:
		return o
	}
}

func TestProperties(t *testing.T) {
	const seed = 42
	rng := rand.New(rand.NewPCG(seed, seed))
	for i := 0; i < 2000; i++ {
		a, b, c := randomVector(rng), randomVector(rng), randomVector(rng)

		if Compare(a, b) != mirror(Compare(b, a)) {
			t.Fatalf("seed %d: Compare not mirrored for %v %v", seed, a, b)
		}
		if Compare(Merge(a, b), Merge(b, a)) != Equal {
			t.Fatalf("seed %d: Merge not commutative for %v %v", seed, a, b)
		}
		if Compare(Merge(Merge(a, b), c), Merge(a, Merge(b, c))) != Equal {
			t.Fatalf("seed %d: Merge not associative for %v %v %v", seed, a, b, c)
		}
		if Compare(Merge(a, a), a) != Equal {
			t.Fatalf("seed %d: Merge not idempotent for %v", seed, a)
		}
		m := Merge(a, b)
		if o := Compare(m, a); o != Equal && o != After {
			t.Fatalf("seed %d: Merge(%v, %v) does not dominate a", seed, a, b)
		}
		if o := Compare(m, b); o != Equal && o != After {
			t.Fatalf("seed %d: Merge(%v, %v) does not dominate b", seed, a, b)
		}
		if Compare(Bump(a, "b"), a) != After {
			t.Fatalf("seed %d: Bump(%v) is not after the input", seed, a)
		}
	}
}
