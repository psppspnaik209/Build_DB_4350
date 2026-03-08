package store

import (
	"strings"
	"testing"
)

func TestStorePersistsAcrossOpen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	storage, err := Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	if err := storage.Set("course", "4350"); err != nil {
		t.Fatalf("set value: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	value, ok := reopened.Get("course")
	if !ok {
		t.Fatal("expected key to exist after reopen")
	}
	if value != "4350" {
		t.Fatalf("got %q, want %q", value, "4350")
	}
}

func TestReplaySkipsMalformedRecords(t *testing.T) {
	t.Parallel()

	idx := newIndex()
	input := strings.NewReader("SET alpha one\ninvalid\nSET beta two\nSET gamma\n")

	if err := replayFrom(input, idx); err != nil {
		t.Fatalf("replay log: %v", err)
	}

	if value, ok := idx.get("alpha"); !ok || value != "one" {
		t.Fatalf("alpha = (%q, %t), want (%q, true)", value, ok, "one")
	}
	if value, ok := idx.get("beta"); !ok || value != "two" {
		t.Fatalf("beta = (%q, %t), want (%q, true)", value, ok, "two")
	}
	if _, ok := idx.get("gamma"); ok {
		t.Fatal("expected malformed record to be ignored")
	}
}
