//go:build !unix

package server

// flockExclusive is a no-op where flock is unavailable (windows, wasm); the
// single-writer guard then relies on the operator running one emulator per
// database file, as before.
func flockExclusive(lockPath string) (func() error, error) {
	return func() error { return nil }, nil
}
