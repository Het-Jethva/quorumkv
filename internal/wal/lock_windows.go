//go:build windows

package wal

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

// acquireDirectoryLock opens path with an empty share mode, which denies every
// other open until this handle closes. Windows closes the handle when this
// process exits for any reason, including a crash, so a restarted Node never
// has to clear a stale lock by hand.
func acquireDirectoryLock(path string) (io.Closer, error) {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("open data directory lock %q: %w", path, err)
	}
	handle, err := syscall.CreateFile(
		name,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0,
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("lock data directory %q (another QuorumKV Node may be using it): %w", path, err)
	}
	return os.NewFile(uintptr(handle), path), nil
}
