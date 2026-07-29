//go:build !windows

package wal

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

// acquireDirectoryLock takes an exclusive advisory lock on path. The lock is
// held by the open file description, so the kernel releases it when this
// process exits for any reason, including a crash. A restarted Node therefore
// never has to clear a stale lock by hand.
func acquireDirectoryLock(path string) (io.Closer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open data directory lock %q: %w", path, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("lock data directory %q (another QuorumKV Node may be using it): %w", path, err)
	}
	return file, nil
}
