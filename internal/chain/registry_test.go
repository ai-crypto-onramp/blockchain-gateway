package chain

import (
	"errors"
	"testing"
)

func TestRegistry_GetKnown(t *testing.T) {
	r := NewRegistry()
	r.Register(NewStubAdapter("ethereum", 64))
	r.Register(NewStubAdapter("polygon", 256))

	a, err := r.Get("ethereum")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.ChainID() != "ethereum" {
		t.Fatalf("ChainID = %q, want ethereum", a.ChainID())
	}
	if a.FinalityBlocks() != 64 {
		t.Fatalf("FinalityBlocks = %d, want 64", a.FinalityBlocks())
	}
}

func TestRegistry_GetUnknown(t *testing.T) {
	r := NewRegistry()
	r.Register(NewStubAdapter("ethereum", 64))

	_, err := r.Get("polygon")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrChainUnknown) {
		t.Fatalf("expected ErrChainUnknown, got %v", err)
	}
}

func TestRegistry_Chains(t *testing.T) {
	r := NewRegistry()
	r.Register(NewStubAdapter("solana", 1))
	r.Register(NewStubAdapter("bitcoin", 6))
	r.Register(NewStubAdapter("ethereum", 64))

	got := r.Chains()
	want := []string{"bitcoin", "ethereum", "solana"}
	if len(got) != len(want) {
		t.Fatalf("Chains = %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("Chains[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestRegistry_DuplicatePanics(t *testing.T) {
	r := NewRegistry()
	r.Register(NewStubAdapter("ethereum", 64))
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on duplicate registration")
		}
	}()
	r.Register(NewStubAdapter("ethereum", 64))
}

func TestRegistry_EmptyIDPanics(t *testing.T) {
	r := NewRegistry()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on empty chain id")
		}
	}()
	r.Register(NewStubAdapter("", 0))
}

func TestRegistry_NilAdapterPanics(t *testing.T) {
	r := NewRegistry()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on nil adapter")
		}
	}()
	r.Register(nil)
}
