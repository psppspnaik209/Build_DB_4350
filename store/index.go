package store

type index struct {
	values map[string]string
}

func newIndex() *index {
	return &index{values: make(map[string]string)}
}

func (idx *index) set(key, value string) {
	idx.values[key] = value
}

func (idx *index) get(key string) (string, bool) {
	value, ok := idx.values[key]
	return value, ok
}

func (idx *index) reset() {
	clear(idx.values)
}
