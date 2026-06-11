//go:build unix

package server

import (
	"fmt"
	"os"
	"syscall"
)

// flockExclusive takes a non-blocking exclusive flock on lockPath (creating
// the file if needed) and returns the release function. flock locks are
// per-open-file-description, so a second open+flock in the SAME process
// conflicts exactly like one from another process; the lock dies with the
// process, so a crashed emulator never leaves a stale lock behind.
func flockExclusive(lockPath string) (func() error, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("lock %s: %w", lockPath, err)
	}
	return func() error {
		// Closing releases the flock; the sidecar file itself is left in
		// place (removing it while another process holds a new lock on the
		// same inode would defeat the exclusion).
		return f.Close()
	}, nil
}
