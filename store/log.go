package store

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// DataFileName is the name of the append-only storage file located in the program directory.
const DataFileName = "data.db"

// logRecordFmt is the on-disk format for a single SET operation.
// Each record is a single text line:  "SET <key> <value>\n"
// This keeps the format human-readable and easy to replay upon restarting.
const logRecordFmt = "SET %s %s\n"

// Log manages append-only writes to the data.db file and can
// replay all recorded operations to rebuild an in-memory index reliably.
type Log struct {
	path string   // absolute or relative path to data.db
	file *os.File // open file handle (append mode)
}

// OpenLog opens (or creates) the active data.db file for continuous appending.
// The caller is strictly responsible for calling Close when the engine halts.
//
// Parameters:
//   - dir: The directory path where data.db should be created or opened.
//
// Returns:
//   - *Log: A populated struct capable of receiving appending writes.
//   - error: An error if the filesystem refuses open operations.
func OpenLog(dir string) (*Log, error) {
	path := dir + string(os.PathSeparator) + DataFileName

	// O_CREATE  – create if missing
	// O_APPEND  – all writes go to the end of the file safely concurrently
	// O_WRONLY  – we only write here; reads use a separate runtime handle
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log %s: %w", path, err)
	}
	return &Log{path: path, file: f}, nil
}

// Append writes a SET record to the log and syncs it to disk immediately,
// ensuring absolute durability even if the process crashes after this call returns.
//
// Parameters:
//   - key: The key associated with the record.
//   - value: The payload to be persisted physically.
//
// Returns:
//   - error: Indicates a filesystem or fsync level failure.
func (l *Log) Append(key, value string) error {
	line := fmt.Sprintf(logRecordFmt, key, value)
	if _, err := fmt.Fprint(l.file, line); err != nil {
		return fmt.Errorf("write log: %w", err)
	}
	// fsync guarantees the record is on durable storage before returning to the caller.
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("sync log: %w", err)
	}
	return nil
}

// Replay reads every record in data.db and replays each SET directly into idx.
// This forcibly rebuilds the in-memory index to the last consistent state.
// An empty or nonexistent file is gracefully treated as a clean slate.
//
// Parameters:
//   - dir: The directory containing the data.db file to be logically swept.
//   - idx: The active memory Index that will receive the mutation calls.
//
// Returns:
//   - error: Will propagate failure if opening the valid file fails.
func Replay(dir string, idx *Index) error {
	path := dir + string(os.PathSeparator) + DataFileName

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		// No existing log — nothing to replay.
		return nil
	}
	if err != nil {
		return fmt.Errorf("open log for replay: %w", err)
	}
	// Ensure the handle is properly released when replay exits.
	defer f.Close()

	return replayFrom(f, idx)
}

// replayFrom sequentially reads log structures from r and functionally applies each SET.
// Lines that are empty or fail string format checks are skipped to protect from torn writes.
//
// Parameters:
//   - r: The generic uniform io.Reader, typically an opened os.File.
//   - idx: Memory Index to receive parsed state operations.
//
// Returns:
//   - error: Underlying scanner-level errors during the sequential loop.
func replayFrom(r io.Reader, idx *Index) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Expected format: SET <key> <value>
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 || !strings.EqualFold(parts[0], "SET") {
			// Skip malformed / incomplete records resulting from OS-level torn writes.
			continue
		}
		idx.Set(parts[1], parts[2])
	}
	return scanner.Err()
}

// Close formally releases the internal OS-level active file handle.
//
// Returns:
//   - error: If closing the logical File descriptor faces an issue.
func (l *Log) Close() error {
	return l.file.Close()
}
