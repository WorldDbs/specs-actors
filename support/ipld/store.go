package ipld

import (
	"context"
	"fmt"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipldcbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/specs-actors/v5/actors/util/adt"
)

// Creates a new, empty, unsynchronized IPLD store in memory.
// This store is appropriate for most kinds of testing.
func NewADTStore(ctx context.Context) adt.Store {
	return adt.WrapBlockStore(ctx, NewBlockStoreInMemory())
}

//
// A basic in-memory block store.
//
type BlockStoreInMemory struct {
	data map[cid.Cid]block.Block
}
var _ ipldcbor.IpldBlockstore = (*BlockStoreInMemory)(nil)

func NewBlockStoreInMemory() *BlockStoreInMemory {
	return &BlockStoreInMemory{make(map[cid.Cid]block.Block)}
}

func (mb *BlockStoreInMemory) Get(c cid.Cid) (block.Block, error) {
	d, ok := mb.data[c]
	if ok {
		return d, nil
	}
	return nil, fmt.Errorf("not found")
}

func (mb *BlockStoreInMemory) Put(b block.Block) error {
	mb.data[b.Cid()] = b
	return nil
}

//
// Synchronized block store wrapper.
//
type SyncBlockStore struct {
	bs ipldcbor.IpldBlockstore
	mu sync.Mutex
}

var _ ipldcbor.IpldBlockstore = (*SyncBlockStore)(nil)

func NewSyncBlockStore(bs ipldcbor.IpldBlockstore) *SyncBlockStore {
	return &SyncBlockStore{
		bs: bs,
	}
}

func (ss *SyncBlockStore) Get(c cid.Cid) (block.Block, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.bs.Get(c)
}

func (ss *SyncBlockStore) Put(b block.Block) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.bs.Put(b)
}

//
// Metric-recording block store wrapper.
//
type MetricsBlockStore struct {
	bs         ipldcbor.IpldBlockstore
	Writes     uint64
	WriteBytes uint64
	Reads      uint64
	ReadBytes  uint64
}
var _ ipldcbor.IpldBlockstore = (*MetricsBlockStore)(nil)

func NewMetricsBlockStore(underlying ipldcbor.IpldBlockstore) *MetricsBlockStore {
	return &MetricsBlockStore{bs: underlying}
}

func (ms *MetricsBlockStore) Get(c cid.Cid) (block.Block, error) {
	ms.Reads++
	blk, err := ms.bs.Get(c)
	if err != nil {
		return blk, err
	}
	ms.ReadBytes += uint64(len(blk.RawData()))
	return blk, nil
}

func (ms *MetricsBlockStore) Put(b block.Block) error {
	ms.Writes++
	ms.WriteBytes += uint64(len(b.RawData()))
	return ms.bs.Put(b)
}

func (ms *MetricsBlockStore) ReadCount() uint64 {
	return ms.Reads
}

func (ms *MetricsBlockStore) WriteCount() uint64 {
	return ms.Writes
}

func (ms *MetricsBlockStore) ReadSize() uint64 {
	return ms.ReadBytes
}

func (ms *MetricsBlockStore) WriteSize() uint64 {
	return ms.WriteBytes
}
