package avl

import (
	"bytes"

	"github.com/perlin-network/wavelet/lru"
	"github.com/perlin-network/wavelet/store"
	"github.com/valyala/bytebufferpool"
)

// TODO: need a better name
type nodeDiffHashMap struct {
	prefix []byte
	kv     store.KV
	cache  *lru.LRU
	viewID uint64
	keys   map[[MerkleHashSize]byte]struct{}
}

func newNodeDiffHashMap(kv store.KV, prefix []byte, viewID uint64) *nodeDiffHashMap {
	return &nodeDiffHashMap{
		prefix: prefix,
		kv:     kv,
		cache:  lru.NewLRU(1024),
		keys:   make(map[[MerkleHashSize]byte]struct{}),
	}
}

func (h *nodeDiffHashMap) WithLRUCache(size *int) *nodeDiffHashMap {
	if size == nil {
		h.cache = nil
	} else {
		h.cache = lru.NewLRU(*size)
	}

	return h
}

func (h *nodeDiffHashMap) Get(id [MerkleHashSize]byte) (*node, error) {
	value, ok := h.cache.Load(id)
	if ok {
		return value.(*node), nil
	}

	if _, ok := h.keys[id]; !ok {
		return nil, nil
	}

	b, err := h.kv.Get(append(h.prefix, id[:]...))
	if err != nil {
		return nil, err
	}

	node, err := DeserializeFromDifference(bytes.NewReader(b), h.viewID)
	if err != nil {
		return nil, err
	}

	h.cache.Put(id, node)
	return node, nil
}

func (h *nodeDiffHashMap) Put(node *node) error {
	h.cache.Put(node.id, node)

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	node.serializeForDifference(buf)

	// Keep track of all keys in the map to avoid querying the KV
	// if the key is not present
	h.keys[node.id] = struct{}{}

	return h.kv.Put(append(h.prefix, node.id[:]...), buf.Bytes())
}
