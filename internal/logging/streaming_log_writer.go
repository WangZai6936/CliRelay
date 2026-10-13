package logging

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/diagnostics"
	log "github.com/sirupsen/logrus"
)

// StreamingLogTruncationNote is appended when async log queues dropped chunks.
// The client still received the full stream; the persisted body did not.
func StreamingLogTruncationNote(dropped int) string {
	return fmt.Sprintf("\n\n[truncated: dropped %d streaming log chunks; persisted response body is incomplete]\n", dropped)
}

// FileStreamingLogWriter spools streaming response chunks to temporary files
// and assembles the final human-readable log only when the stream closes.
type FileStreamingLogWriter struct {
	logger               *FileRequestLogger
	logFilePath          string
	url                  string
	method               string
	timestamp            time.Time
	requestHeaders       map[string][]string
	requestBodyPath      string
	responseBodyPath     string
	responseBodyFile     *os.File
	chunkChan            chan []byte
	closeChan            chan struct{}
	errorChan            chan error
	responseStatus       int
	statusWritten        bool
	responseHeaders      map[string][]string
	apiRequest           []byte
	apiResponse          []byte
	apiResponseTimestamp time.Time
	droppedChunks        atomic.Int64
}

func (w *FileStreamingLogWriter) WriteChunkAsync(chunk []byte) {
	if w.chunkChan == nil {
		return
	}

	chunkCopy := make([]byte, len(chunk))
	copy(chunkCopy, chunk)
	select {
	case w.chunkChan <- chunkCopy:
	default:
		w.droppedChunks.Add(1)
	}
}

func (w *FileStreamingLogWriter) NoteDroppedChunks(n int) {
	if n > 0 {
		w.droppedChunks.Add(int64(n))
	}
}

func (w *FileStreamingLogWriter) WriteStatus(status int, headers map[string][]string) error {
	if status == 0 {
		return nil
	}
	w.responseStatus = status
	if headers != nil {
		w.responseHeaders = make(map[string][]string, len(headers))
		for key, values := range headers {
			headerValues := make([]string, len(values))
			copy(headerValues, values)
			w.responseHeaders[key] = headerValues
		}
	}
	w.statusWritten = true
	return nil
}

func (w *FileStreamingLogWriter) WriteAPIRequest(apiRequest []byte) error {
	if len(apiRequest) == 0 {
		return nil
	}
	w.apiRequest = bytes.Clone(apiRequest)
	return nil
}

func (w *FileStreamingLogWriter) WriteAPIResponse(apiResponse []byte) error {
	if len(apiResponse) == 0 {
		return nil
	}
	w.apiResponse = bytes.Clone(apiResponse)
	return nil
}

func (w *FileStreamingLogWriter) SetFirstChunkTimestamp(timestamp time.Time) {
	if !timestamp.IsZero() {
		w.apiResponseTimestamp = timestamp
	}
}

func (w *FileStreamingLogWriter) Close() error {
	if w.chunkChan != nil {
		close(w.chunkChan)
	}
	if w.closeChan != nil {
		<-w.closeChan
		w.chunkChan = nil
	}

	w.appendTruncationNote()

	select {
	case errWrite := <-w.errorChan:
		w.cleanupTempFiles()
		return errWrite
	default:
	}

	if w.logFilePath == "" {
		w.cleanupTempFiles()
		return nil
	}

	logFile, errOpen := openActiveLogFile(w.logFilePath)
	if errOpen != nil {
		w.cleanupTempFiles()
		return fmt.Errorf("failed to create log file: %w", errOpen)
	}
	releaseFile := releaseActiveLogFileOnce(w.logFilePath)
	defer releaseFile()

	writeErr := w.writeFinalLog(logFile)
	if errClose := logFile.Close(); errClose != nil {
		log.WithError(errClose).Warn("failed to close request log file")
		if writeErr == nil {
			writeErr = errClose
		}
	}

	w.cleanupTempFiles()
	releaseFile()
	// Retain only completed logs, after the final file has been closed.
	if writeErr == nil && w.logger != nil {
		if errCleanup := w.logger.cleanupOldRequestLogs(false); errCleanup != nil {
			log.WithError(errCleanup).Warn("failed to cleanup old request logs")
		}
	}
	return writeErr
}

func (w *FileStreamingLogWriter) asyncWriter() {
	defer close(w.closeChan)

	for chunk := range w.chunkChan {
		if w.responseBodyFile == nil {
			continue
		}
		if _, errWrite := w.responseBodyFile.Write(chunk); errWrite != nil {
			select {
			case w.errorChan <- errWrite:
			default:
			}
			if errClose := w.responseBodyFile.Close(); errClose != nil {
				select {
				case w.errorChan <- errClose:
				default:
				}
			}
			w.responseBodyFile = nil
		}
	}

	if w.responseBodyFile == nil {
		return
	}
	if errClose := w.responseBodyFile.Close(); errClose != nil {
		select {
		case w.errorChan <- errClose:
		default:
		}
	}
	w.responseBodyFile = nil
}

func (w *FileStreamingLogWriter) writeFinalLog(logFile *os.File) error {
	if errWrite := writeRequestInfoWithBody(logFile, w.url, w.method, w.requestHeaders, nil, w.requestBodyPath, w.timestamp, diagnostics.Snapshot{}); errWrite != nil {
		return errWrite
	}
	if errWrite := writeAPISection(logFile, "=== API REQUEST ===\n", "=== API REQUEST", w.apiRequest, time.Time{}); errWrite != nil {
		return errWrite
	}
	if errWrite := writeAPISection(logFile, "=== API RESPONSE ===\n", "=== API RESPONSE", w.apiResponse, w.apiResponseTimestamp); errWrite != nil {
		return errWrite
	}

	responseBodyFile, errOpen := os.Open(w.responseBodyPath)
	if errOpen != nil {
		return errOpen
	}
	defer func() {
		if errClose := responseBodyFile.Close(); errClose != nil {
			log.WithError(errClose).Warn("failed to close response body temp file")
		}
	}()

	return writeResponseSection(logFile, w.responseStatus, w.statusWritten, w.responseHeaders, responseBodyFile, nil, false)
}

func (w *FileStreamingLogWriter) appendTruncationNote() {
	dropped := int(w.droppedChunks.Load())
	if dropped <= 0 || w.responseBodyPath == "" {
		return
	}
	log.WithField("dropped_chunks", dropped).Warn("streaming request log dropped chunks; persisted body is incomplete")
	noteFile, errOpen := os.OpenFile(w.responseBodyPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if errOpen != nil {
		log.WithError(errOpen).Warn("failed to append streaming log truncation note")
		return
	}
	if _, errWrite := noteFile.WriteString(StreamingLogTruncationNote(dropped)); errWrite != nil {
		log.WithError(errWrite).Warn("failed to write streaming log truncation note")
	}
	if errClose := noteFile.Close(); errClose != nil {
		log.WithError(errClose).Warn("failed to close streaming log truncation note")
	}
}

func (w *FileStreamingLogWriter) cleanupTempFiles() {
	if w.requestBodyPath != "" {
		if errRemove := removeOwnedLogTemp(w.requestBodyPath); errRemove != nil {
			log.WithError(errRemove).Warn("failed to remove request body temp file")
		}
		w.requestBodyPath = ""
	}
	if w.responseBodyPath != "" {
		if errRemove := removeOwnedLogTemp(w.responseBodyPath); errRemove != nil {
			log.WithError(errRemove).Warn("failed to remove response body temp file")
		}
		w.responseBodyPath = ""
	}
}

// NoOpStreamingLogWriter preserves the streaming logging contract when request
// logging is disabled.
type NoOpStreamingLogWriter struct{}

func (w *NoOpStreamingLogWriter) WriteChunkAsync(_ []byte) {}

func (w *NoOpStreamingLogWriter) NoteDroppedChunks(_ int) {}

func (w *NoOpStreamingLogWriter) WriteStatus(_ int, _ map[string][]string) error {
	return nil
}

func (w *NoOpStreamingLogWriter) WriteAPIRequest(_ []byte) error {
	return nil
}

func (w *NoOpStreamingLogWriter) WriteAPIResponse(_ []byte) error {
	return nil
}

func (w *NoOpStreamingLogWriter) SetFirstChunkTimestamp(_ time.Time) {}

func (w *NoOpStreamingLogWriter) Close() error { return nil }
