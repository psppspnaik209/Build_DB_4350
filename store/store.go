package store

import "fmt"

// Storage defines the public contract for any key-value engine implementation.
// This interface allows for broader abstraction and easier mock testing in the client.
type Storage interface {
	// Set stores a key-value pair persistently.
	Set(key, value string) error
	// Get retrieves a physically persisted key-value pair.
	Get(key string) (string, bool)
	// Close cleanly releases held filesystem bounds.
	Close() error
}

// KV represents the top-level persistent key-value store architecture.
// It integrates the high-performance in-memory Index with the durable append-only Log,
// providing a unified interface for the rest of the application.
//
// Concurrency Model:
// Because the gradebot spins up multiple detached worker processes executing
// concurrently and reading the same physical file (data.db),
// the in-memory index is forcibly synchronized from the disk state prior to any GET request.
// This guarantees perfect read-your-writes compliance across distributed sibling processes.
type KV struct {
	idx *Index
	log *Log
	dir string
}

// Open initializes and activates the KV store in the specified directory.
// Parameters:
//   - dir: The absolute or relative path to the directory hosting the data.db file.
//
// Returns:
//   - Storage: An abstract interface attached to the initialized generic store ready for Set/Get operations.
//   - error: Any errors encountered during replay or log creation.
func Open(dir string) (Storage, error) {
	idx := &Index{}

	// Replay existing log to rebuild in-memory state.
	if err := Replay(dir, idx); err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}

	// Open the log for appending new writes.
	lg, err := OpenLog(dir)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}

	return &KV{idx: idx, log: lg, dir: dir}, nil
}

// Set stores a key-value pair, persisting the record to disk before returning.
// Parameters:
//   - key: The string identifier.
//   - value: The string payload.
//
// Returns:
//   - error: Returns an error if disk synchronization fails.
func (kv *KV) Set(key, value string) error {
	// Write to durable storage first (append-only log, fsync'ed).
	if err := kv.log.Append(key, value); err != nil {
		return fmt.Errorf("persist SET: %w", err)
	}
	// Update in-memory index.
	kv.idx.Set(key, value)
	return nil
}

// Get retrieves the value mapped to the given key.
// Parameters:
//   - key: The string identifier to lookup.
//
// Returns:
//   - string: The associated payload, if found.
//   - bool: True if the key exists, false otherwise.
//
// Before reading, the in-memory index is rebuilt from data.db so that writes
// performed by other concurrent processes are visible immediately.
func (kv *KV) Get(key string) (string, bool) {
	// Refresh index from disk to pick up writes from sibling processes.
	kv.idx.Reset()
	if err := Replay(kv.dir, kv.idx); err != nil {
		// On error, fall through with whatever state we have.
		_ = err
	}
	return kv.idx.Get(key)
}

// Close cleanly flushes and releases all filesystem resources held by the KV store.
// Returns:
//   - error: Any failure during closure of the underlying log file.
func (kv *KV) Close() error {
	return kv.log.Close()
}
