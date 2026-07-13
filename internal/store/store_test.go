package store

import "testing"

func TestErrNotFoundError(t *testing.T) {
	e := &ErrNotFound{Chain: "ethereum", Key: "0x1"}
	if e.Error() != "not found: ethereum/0x1" {
		t.Errorf("Error(): %q want %q", e.Error(), "not found: ethereum/0x1")
	}
}