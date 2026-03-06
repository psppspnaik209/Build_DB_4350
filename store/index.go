// Package store implements the in-memory key-value index and append-only
// log persistence for the kvstore project.
package store

// Entry holds a single key-value pair in the in-memory index.
type Entry struct {
	Key   string
	Value string
}

// Index is a custom in-memory key-value store backed by a slice of entries.
// It does NOT use any built-in map type.
// Semantics: last-write-wins — a later Set for the same key replaces
// the value of the existing entry.
type Index struct {
	Entries []Entry
}

// Set inserts or updates the value for key.
// Parameters:
//   - key: the string identifier to associate with the given value.
//   - value: the string payload to persist in the index.
// If the key already exists it is overwritten in-place (last-write-wins).
func (idx *Index) Set(key, value string) {
	for i := range idx.Entries {
		if idx.Entries[i].Key == key {
			idx.Entries[i].Value = value
			return
		}
	}
	idx.Entries = append(idx.Entries, Entry{Key: key, Value: value})
}

// Get retrieves the value for key.
// Parameters:
//   - key: the string identifier to lookup in the index.
// Returns:
//   - string: the found value, if any.
//   - bool: false if the key does not exist, true otherwise.
func (idx *Index) Get(key string) (string, bool) {
	for i := len(idx.Entries) - 1; i >= 0; i-- {
		if idx.Entries[i].Key == key {
			return idx.Entries[i].Value, true
		}
	}
	return "", false
}

// Reset clears all entries from the index, allowing it to be rebuilt
// from a fresh log replay without allocating a new Index.
func (idx *Index) Reset() {
	idx.Entries = idx.Entries[:0]
}
