package store

import "fmt"

// Storage exposes the persistence operations used by the command loop.
type Storage interface {
	Set(key, value string) error
	Get(key string) (string, bool)
	Close() error
}

type kvStore struct {
	index *index
	log   *logFile
	dir   string
}

// Open creates a store backed by `data.db` in dir.
func Open(dir string) (Storage, error) {
	store := &kvStore{index: newIndex(), dir: dir}

	if err := store.refresh(); err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}

	logFile, err := openLog(dir)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}
	store.log = logFile

	return store, nil
}

func (kv *kvStore) Set(key, value string) error {
	if err := kv.log.append(key, value); err != nil {
		return fmt.Errorf("persist set: %w", err)
	}

	kv.index.set(key, value)
	return nil
}

func (kv *kvStore) Get(key string) (string, bool) {
	if err := kv.refresh(); err != nil {
		return kv.index.get(key)
	}

	return kv.index.get(key)
}

func (kv *kvStore) Close() error {
	return kv.log.close()
}

func (kv *kvStore) refresh() error {
	refreshed := newIndex()
	if err := replay(kv.dir, refreshed); err != nil {
		return err
	}

	kv.index = refreshed
	return nil
}
