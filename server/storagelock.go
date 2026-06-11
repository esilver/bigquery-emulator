package server

import (
	"fmt"
	"strings"
)

// Exclusive storage lock (issue #14).
//
// Native DuckDB flocks its database file, so a second process opening the
// same database fails fast with "database is locked". The pure-Go engine
// runs DuckDB's filesystem through a WASI shim that (like DuckDB's own
// emscripten target) carries NO file locking at all: two emulator processes
// pointed at one --database file would interleave page and WAL writes with
// zero protection, and the corruption surfaces later as engine crashes or
// nonterminating statements (the #14 livelock class), not as a clean error
// at the second open. The same applies to one process accidentally opening
// the file through two storage handles.
//
// lockStorage restores the fail-fast: an OS-level exclusive lock (flock on
// unix) on a sidecar "<dbpath>.emulator.lock" file, taken in New and held
// for the server's lifetime. A second New on the same path — same process
// or another one — errors immediately instead of corrupting.

// storageFilePath extracts the filesystem path from a storage DSN
// ("file:/path?cache=shared", "file:/path", plain "/path"); it returns ""
// for in-memory storages, which need no lock.
func storageFilePath(storage Storage) string {
	s := string(storage)
	if s == "" || s == ":memory:" || strings.HasPrefix(s, "file::memory:") || strings.Contains(s, "mode=memory") {
		return ""
	}
	s = strings.TrimPrefix(s, "file:")
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s = s[:i]
	}
	if s == "" || s == ":memory:" {
		return ""
	}
	return s
}

// lockStorage acquires the exclusive lock for the storage path and returns
// the unlock function. It is a no-op for in-memory storage.
func lockStorage(storage Storage) (func() error, error) {
	path := storageFilePath(storage)
	if path == "" {
		return func() error { return nil }, nil
	}
	unlock, err := flockExclusive(path + ".emulator.lock")
	if err != nil {
		return nil, fmt.Errorf(
			"database %q is already in use by another bigquery-emulator instance "+
				"(the pure-Go DuckDB engine has no file locking of its own, and two "+
				"writers on one database file corrupt it): %w", path, err)
	}
	return unlock, nil
}
