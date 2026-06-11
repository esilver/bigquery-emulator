//go:build unix

package server_test

// Single-writer storage-lock regression (issue #14): the pure-Go DuckDB
// engine carries no file locking (native DuckDB flocks its database file),
// so two emulator instances on one database file would interleave page/WAL
// writes and corrupt it — surfacing later as the #14 engine livelock, not
// as a clean error. server.New must therefore fail fast on a second open.

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
)

func TestStorageLockRejectsSecondInstance(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "locked.db")
	storage := server.Storage(fmt.Sprintf("file:%s?cache=shared", dbPath))

	first, err := server.New(storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Load(server.StructSource(types.NewProject("test"))); err != nil {
		t.Fatal(err)
	}

	if second, err := server.New(storage); err == nil {
		second.Stop(ctx)
		first.Stop(ctx)
		t.Fatal("second server.New on the same database file succeeded; the single-writer lock is not held")
	} else if !strings.Contains(err.Error(), "already in use") {
		t.Fatalf("second open failed, but not with the lock error: %v", err)
	}

	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	// After the holder closes, the file is reusable.
	third, err := server.New(storage)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	if err := third.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
