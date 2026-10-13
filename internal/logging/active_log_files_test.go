package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActiveStreamSpoolsSurviveSizeCleanup(t *testing.T) {
	dir := t.TempDir()
	logger := NewFileRequestLogger(true, dir, "", 10)
	writer, err := logger.LogStreamingRequest("/v1/chat/completions", "POST", nil, []byte(strings.Repeat("request", 40)), "active-stream")
	if err != nil {
		t.Fatal(err)
	}
	writer.WriteChunkAsync([]byte("data: completed\n\n"))
	if err := writer.WriteStatus(200, nil); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(dir, "request-body-orphan.tmp")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := enforceLogDirSizeLimit(dir, 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan remains: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("cleanup removed an active spool: %v", err)
	}
	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(logs) != 1 {
		t.Fatalf("completed logs: %v, %v", logs, err)
	}
	content, err := os.ReadFile(logs[0])
	if err != nil || !strings.Contains(string(content), "data: completed") {
		t.Fatalf("incomplete final log: %v", err)
	}
	if _, err := enforceLogDirSizeLimit(dir, 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(logs[0]); !os.IsNotExist(err) {
		t.Fatalf("completed log should become eligible: %v", err)
	}
}

func TestActiveFinalLogSurvivesSizeAndCountCleanup(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "error-active.log")
	file, err := openActiveLogFile(active)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	defer releaseActiveLogFile(active)
	if _, err := file.WriteString(strings.Repeat("active", 20)); err != nil {
		t.Fatal(err)
	}
	if _, err := enforceLogDirSizeLimit(dir, 1, ""); err != nil {
		t.Fatal(err)
	}
	logger := NewFileRequestLogger(true, dir, "", 1)
	newer := filepath.Join(dir, "error-newer.log")
	if err := os.WriteFile(newer, []byte("newer"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := logger.cleanupOldErrorLogs(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("active final log was removed: %v", err)
	}
}
