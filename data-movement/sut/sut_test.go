package sut

import (
	"slices"
	"testing"
)

func TestSlingRegistered(t *testing.T) {
	if !slices.Contains(Names, "sling") {
		t.Fatal("sling is not listed in Names")
	}
	s := New("sling", "pg-pg")
	if s == nil || s.Name() != "sling" {
		t.Fatalf("New(sling) = %#v", s)
	}
	for _, route := range []string{"pg-pg", "pg-mysql", "mysql-mysql", "mysql-pg"} {
		if !slices.Contains(s.Routes(), route) {
			t.Errorf("sling route list is missing %s", route)
		}
	}
}
