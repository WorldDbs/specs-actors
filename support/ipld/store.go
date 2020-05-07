package ipld

import (
	"context"
	"fmt"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type BlockStoreInMemory struct {
	data map[cid.Cid]block.Block
}

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

// Creates a new, empty IPLD store in memory.
func NewADTStore(ctx context.Context) adt.Store {
	return adt.WrapStore(ctx, cbor.NewCborStore(NewBlockStoreInMemory()))

}

type SyncBlockStoreInMemory struct {
	bs *BlockStoreInMemory
	mu sync.Mutex
}

func NewSyncBlockStoreInMemory() *SyncBlockStoreInMemory {
	return &SyncBlockStoreInMemory{
		bs: NewBlockStoreInMemory(),
	}
}

func (ss *SyncBlockStoreInMemory) Get(c cid.Cid) (block.Block, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.bs.Get(c)
}

func (ss *SyncBlockStoreInMemory) Put(b block.Block) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.bs.Put(b)
}

// Creates a new, threadsafe, empty IPLD store in memory
func NewSyncADTStore(ctx context.Context) adt.Store {
	return adt.WrapStore(ctx, cbor.NewCborStore(NewSyncBlockStoreInMemory()))
}

type MetricsBlockStore struct {
	bs         cbor.IpldBlockstore
	Writes     uint64
	WriteBytes uint64
	Reads      uint64
	ReadBytes  uint64
}

func NewMetricsBlockStore(underlying cbor.IpldBlockstore) *MetricsBlockStore {
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
