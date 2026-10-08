package main

import (
	"strconv"
	"testing"
)

func TestCandidateSeedUsernameMatchesDriverDecimalNames(t *testing.T) {
	for i := 0; i <= 500; i++ {
		suffix := strconv.Itoa(i)
		if got, want := seededUsername(suffix), "Cornell_"+suffix; got != want {
			t.Fatalf("user %d: %s, want %s", i, got, want)
		}
	}
}
