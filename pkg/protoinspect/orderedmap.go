package protoinspect

// OrderedMap is a map that preserves key insertion order during iteration.
type OrderedMap[K comparable, V any] struct {
	keys   []K
	values map[K]V
}

// NewOrderedMap returns an empty OrderedMap.
func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{values: make(map[K]V)}
}

// Set inserts or updates the value for k. Updating an existing key does not
// change its position in iteration order.
func (m *OrderedMap[K, V]) Set(k K, v V) {
	if _, exists := m.values[k]; !exists {
		m.keys = append(m.keys, k)
	}
	m.values[k] = v
}

// Get returns the value for k and whether it was present.
func (m *OrderedMap[K, V]) Get(k K) (V, bool) {
	v, ok := m.values[k]
	return v, ok
}

// Keys returns the keys in insertion order.
func (m *OrderedMap[K, V]) Keys() []K {
	return m.keys
}

// Len returns the number of entries.
func (m *OrderedMap[K, V]) Len() int {
	return len(m.keys)
}

// Range calls f for each entry in insertion order, stopping early if f returns false.
func (m *OrderedMap[K, V]) Range(f func(k K, v V) bool) {
	for _, k := range m.keys {
		if !f(k, m.values[k]) {
			return
		}
	}
}
