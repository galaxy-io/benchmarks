package sling

import (
	"strings"
	"testing"
)

func TestTransferScriptRunsOneFullRefreshPerTable(t *testing.T) {
	script := transferScript([]string{"orders", "lineitem"})

	if got := strings.Count(script, "sling run "); got != 2 {
		t.Fatalf("sling command count = %d, want 2\n%s", got, script)
	}
	for _, object := range []string{"'bench.orders'", "'bench.lineitem'"} {
		if got := strings.Count(script, object); got != 2 {
			t.Errorf("object %s count = %d, want source and target", object, got)
		}
	}
	if !strings.Contains(script, "--mode full-refresh") {
		t.Errorf("script does not select full refresh:\n%s", script)
	}
	if !strings.Contains(script, `pids="$pids $!"`) || !strings.Contains(script, "wait $p || fail=1") {
		t.Errorf("script does not wait for every parallel transfer:\n%s", script)
	}
}

func TestShellQuote(t *testing.T) {
	if got, want := shellQuote("bench.o'rders"), `'bench.o'"'"'rders'`; got != want {
		t.Fatalf("shellQuote() = %q, want %q", got, want)
	}
}
