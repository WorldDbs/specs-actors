package agent

import (
	"context"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/dline"
	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	miner3 "github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v4/actors/util/adt"
	cid "github.com/ipfs/go-cid"
)

type MinerStateV2 struct {
	Root cid.Cid
	Ctx  context.Context
	st   *miner2.State
}

func (m *MinerStateV2) state(store adt.Store) (*miner2.State, error) {
	if m.st == nil {
		var st miner2.State
		err := store.Get(m.Ctx, m.Root, &st)
		if err != nil {
			return nil, err
		}
		m.st = &st
	}
	return m.st, nil
}

func (m *MinerStateV2) HasSectorNo(store adt.Store, sectorNo abi.SectorNumber) (bool, error) {
	st, err := m.state(store)
	if err != nil {
		return false, err
	}
	return st.HasSectorNo(store, sectorNo)
}

func (m *MinerStateV2) FindSector(store adt.Store, sectorNo abi.SectorNumber) (uint64, uint64, error) {
	st, err := m.state(store)
	if err != nil {
		return 0, 0, err
	}
	return st.FindSector(store, sectorNo)
}

func (m *MinerStateV2) ProvingPeriodStart(store adt.Store) (abi.ChainEpoch, error) {
	st, err := m.state(store)
	if err != nil {
		return 0, err
	}
	return st.ProvingPeriodStart, nil
}

func (m *MinerStateV2) LoadSectorInfo(store adt.Store, sectorNo uint64) (SimSectorInfo, error) {
	st, err := m.state(store)
	if err != nil {
		return nil, err
	}
	sectors, err := st.LoadSectorInfos(store, bitfield.NewFromSet([]uint64{uint64(sectorNo)}))
	if err != nil {
		return nil, err
	}
	return &SectorInfoV2{info: sectors[0]}, nil
}

func (m *MinerStateV2) DeadlineInfo(store adt.Store, currEpoch abi.ChainEpoch) (*dline.Info, error) {
	st, err := m.state(store)
	if err != nil {
		return nil, err
	}
	return st.DeadlineInfo(currEpoch), nil
}

func (m *MinerStateV2) FeeDebt(store adt.Store) (abi.TokenAmount, error) {
	st, err := m.state(store)
	if err != nil {
		return big.Zero(), err
	}
	return st.FeeDebt, nil
}

func (m *MinerStateV2) LoadDeadlineState(store adt.Store, dlIdx uint64) (SimDeadlineState, error) {
	st, err := m.state(store)
	if err != nil {
		return nil, err
	}
	dls, err := st.LoadDeadlines(store)
	if err != nil {
		return nil, err
	}
	dline, err := dls.LoadDeadline(store, dlIdx)
	if err != nil {
		return nil, err
	}
	return &DeadlineStateV2{deadline: dline}, nil
}

type DeadlineStateV2 struct {
	deadline *miner2.Deadline
}

func (d *DeadlineStateV2) LoadPartition(store adt.Store, partIdx uint64) (SimPartitionState, error) {
	part, err := d.deadline.LoadPartition(store, partIdx)
	if err != nil {
		return nil, err
	}
	return &PartitionStateV2{partition: part}, nil
}

type PartitionStateV2 struct {
	partition *miner2.Partition
}

func (p *PartitionStateV2) Terminated() bitfield.BitField {
	return p.partition.Terminated
}

type SectorInfoV2 struct {
	info *miner2.SectorOnChainInfo
}

func (s *SectorInfoV2) Expiration() abi.ChainEpoch {
	return s.info.Expiration
}

type MinerStateV3 struct {
	Root cid.Cid
	st   *miner3.State
	Ctx  context.Context
}

func (m *MinerStateV3) state(store adt.Store) (*miner3.State, error) {
	if m.st == nil {
		var st miner3.State
		err := store.Get(m.Ctx, m.Root, &st)
		if err != nil {
			return nil, err
		}
		m.st = &st
	}
	return m.st, nil
}

func (m *MinerStateV3) HasSectorNo(store adt.Store, sectorNo abi.SectorNumber) (bool, error) {
	st, err := m.state(store)
	if err != nil {
		return false, err
	}
	return st.HasSectorNo(store, sectorNo)
}

func (m *MinerStateV3) FindSector(store adt.Store, sectorNo abi.SectorNumber) (uint64, uint64, error) {
	st, err := m.state(store)
	if err != nil {
		return 0, 0, err
	}
	return st.FindSector(store, sectorNo)
}

func (m *MinerStateV3) ProvingPeriodStart(store adt.Store) (abi.ChainEpoch, error) {
	st, err := m.state(store)
	if err != nil {
		return 0, err
	}
	return st.ProvingPeriodStart, nil
}

func (m *MinerStateV3) LoadSectorInfo(store adt.Store, sectorNo uint64) (SimSectorInfo, error) {
	st, err := m.state(store)
	if err != nil {
		return nil, err
	}
	sectors, err := st.LoadSectorInfos(store, bitfield.NewFromSet([]uint64{uint64(sectorNo)}))
	if err != nil {
		return nil, err
	}
	return &SectorInfoV3{info: sectors[0]}, nil
}

func (m *MinerStateV3) DeadlineInfo(store adt.Store, currEpoch abi.ChainEpoch) (*dline.Info, error) {
	st, err := m.state(store)
	if err != nil {
		return nil, err
	}
	return st.DeadlineInfo(currEpoch), nil
}

func (m *MinerStateV3) FeeDebt(store adt.Store) (abi.TokenAmount, error) {
	st, err := m.state(store)
	if err != nil {
		return big.Zero(), err
	}
	return st.FeeDebt, nil
}

func (m *MinerStateV3) LoadDeadlineState(store adt.Store, dlIdx uint64) (SimDeadlineState, error) {
	st, err := m.state(store)
	if err != nil {
		return nil, err
	}
	dls, err := st.LoadDeadlines(store)
	if err != nil {
		return nil, err
	}
	dline, err := dls.LoadDeadline(store, dlIdx)
	if err != nil {
		return nil, err
	}
	return &DeadlineStateV3{deadline: dline}, nil
}

type DeadlineStateV3 struct {
	deadline *miner3.Deadline
}

func (d *DeadlineStateV3) LoadPartition(store adt.Store, partIdx uint64) (SimPartitionState, error) {
	part, err := d.deadline.LoadPartition(store, partIdx)
	if err != nil {
		return nil, err
	}
	return &PartitionStateV3{partition: part}, nil
}

type PartitionStateV3 struct {
	partition *miner3.Partition
}

func (p *PartitionStateV3) Terminated() bitfield.BitField {
	return p.partition.Terminated
}

type SectorInfoV3 struct {
	info *miner3.SectorOnChainInfo
}

func (s *SectorInfoV3) Expiration() abi.ChainEpoch {
	return s.info.Expiration
}
