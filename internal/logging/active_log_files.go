package logging

import (
	"os"
	"path/filepath"
	"sync"
)

// Creation, registration and unlinking share a lock: a cleaner must never see
// a new spool file before its writer has registered ownership of the path.
var activeLogFiles = struct {
	sync.Mutex
	paths map[string]int
}{paths: make(map[string]int)}

func logPathIdentity(path string) string {
	if absolute, err := filepath.Abs(path); err == nil {
		return filepath.Clean(absolute)
	}
	return filepath.Clean(path)
}

func createActiveLogTemp(dir, pattern string) (*os.File, error) {
	activeLogFiles.Lock()
	defer activeLogFiles.Unlock()
	file, err := os.CreateTemp(dir, pattern)
	if err == nil {
		activeLogFiles.paths[logPathIdentity(file.Name())]++
	}
	return file, err
}

func openActiveLogFile(path string) (*os.File, error) {
	activeLogFiles.Lock()
	defer activeLogFiles.Unlock()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err == nil {
		activeLogFiles.paths[logPathIdentity(path)]++
	}
	return file, err
}

func releaseActiveLogFile(path string) {
	activeLogFiles.Lock()
	defer activeLogFiles.Unlock()
	identity := logPathIdentity(path)
	if activeLogFiles.paths[identity] > 1 {
		activeLogFiles.paths[identity]--
	} else {
		delete(activeLogFiles.paths, identity)
	}
}

func releaseActiveLogFileOnce(path string) func() {
	return sync.OnceFunc(func() { releaseActiveLogFile(path) })
}

func removeOwnedLogTemp(path string) error {
	activeLogFiles.Lock()
	defer activeLogFiles.Unlock()
	err := os.Remove(path)
	delete(activeLogFiles.paths, logPathIdentity(path))
	return err
}

func removeInactiveLogFile(path string) (bool, error) {
	activeLogFiles.Lock()
	defer activeLogFiles.Unlock()
	if activeLogFiles.paths[logPathIdentity(path)] > 0 {
		return false, nil
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
