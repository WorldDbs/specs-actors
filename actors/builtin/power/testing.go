package power

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type MinerCronEvent struct {
	Epoch   abi.ChainEpoch
	Payload []byte
}

type CronEventsByAddress map[address.Address][]MinerCronEvent
type ClaimsByAddress map[address.Address]Claim
type ProofsByAddress map[address.Address][]proof.SealVerifyInfo

type StateSummary struct {
	Crons  CronEventsByAddress
	Claims ClaimsByAddress
	Proofs ProofsByAddress
}

// Checks internal invariants of power state.
func CheckStateInvariants(st *State, store adt.Store) (*StateSummary, *builtin.MessageAccumulator) {
	acc := &builtin.MessageAccumulator{}

	// basic invariants around recorded power
	acc.Require(st.TotalRawBytePower.GreaterThanEqual(big.Zero()), "total raw power is negative %v", st.TotalRawBytePower)
	acc.Require(st.TotalQualityAdjPower.GreaterThanEqual(big.Zero()), "total qa power is negative %v", st.TotalQualityAdjPower)
	acc.Require(st.TotalBytesCommitted.GreaterThanEqual(big.Zero()), "total raw power committed is negative %v", st.TotalBytesCommitted)
	acc.Require(st.TotalQABytesCommitted.GreaterThanEqual(big.Zero()), "total qa power committed is negative %v", st.TotalQABytesCommitted)

	acc.Require(st.TotalRawBytePower.LessThanEqual(st.TotalQualityAdjPower),
		"total raw power %v is greater than total quality adjusted power %v", st.TotalRawBytePower, st.TotalQualityAdjPower)
	acc.Require(st.TotalBytesCommitted.LessThanEqual(st.TotalQABytesCommitted),
		"committed raw power %v is greater than committed quality adjusted power %v", st.TotalBytesCommitted, st.TotalQABytesCommitted)
	acc.Require(st.TotalRawBytePower.LessThanEqual(st.TotalBytesCommitted),
		"total raw power %v is greater than raw power committed %v", st.TotalRawBytePower, st.TotalBytesCommitted)
	acc.Require(st.TotalQualityAdjPower.LessThanEqual(st.TotalQABytesCommitted),
		"total qua power %v is greater than qa power committed %v", st.TotalQualityAdjPower, st.TotalQABytesCommitted)

	crons := CheckCronInvariants(st, store, acc)
	claims := CheckClaimInvariants(st, store, acc)
	proofs := CheckProofValidationInvariants(st, store, claims, acc)

	return &StateSummary{
		Crons:  crons,
		Claims: claims,
		Proofs: proofs,
	}, acc
}

func CheckCronInvariants(st *State, store adt.Store, acc *builtin.MessageAccumulator) CronEventsByAddress {
	byAddress := make(CronEventsByAddress)
	queue, err := adt.AsMultimap(store, st.CronEventQueue)
	if err != nil {
		acc.Addf("error loading cron event queue: %v", err)
		// Bail here.
		return byAddress
	}

	err = queue.ForAll(func(ekey string, arr *adt.Array) error {
		epoch, err := abi.ParseIntKey(ekey)
		acc.Require(err == nil, "non-int key in cron array")
		if err != nil {
			return nil // error noted above
		}

		acc.Require(abi.ChainEpoch(epoch) >= st.FirstCronEpoch, "cron event at epoch %d before FirstCronEpoch %d",
			epoch, st.FirstCronEpoch)

		var event CronEvent
		return arr.ForEach(&event, func(i int64) error {
			byAddress[event.MinerAddr] = append(byAddress[event.MinerAddr], MinerCronEvent{
				Epoch:   abi.ChainEpoch(epoch),
				Payload: event.CallbackPayload,
			})

			return nil
		})
	})
	acc.RequireNoError(err, "error iterating cron tasks")
	return byAddress
}

func CheckClaimInvariants(st *State, store adt.Store, acc *builtin.MessageAccumulator) ClaimsByAddress {
	byAddress := make(ClaimsByAddress)
	claims, err := adt.AsMap(store, st.Claims)
	if err != nil {
		acc.Addf("error loading power claims: %v", err)
		// Bail here
		return byAddress
	}

	committedRawPower := abi.NewStoragePower(0)
	committedQAPower := abi.NewStoragePower(0)
	rawPower := abi.NewStoragePower(0)
	qaPower := abi.NewStoragePower(0)
	claimsWithSufficientPowerCount := int64(0)
	var claim Claim
	err = claims.ForEach(&claim, func(key string) error {
		addr, err := address.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		byAddress[addr] = claim
		committedRawPower = big.Add(committedRawPower, claim.RawBytePower)
		committedQAPower = big.Add(committedQAPower, claim.QualityAdjPower)

		minPower, err := builtin.ConsensusMinerMinPower(claim.SealProofType)
		acc.Require(err == nil, "could not get consensus miner min power for miner %v: %v", addr, err)
		if err != nil {
			return nil // noted above
		}

		if claim.RawBytePower.GreaterThanEqual(minPower) {
			claimsWithSufficientPowerCount += 1
			rawPower = big.Add(rawPower, claim.RawBytePower)
			qaPower = big.Add(qaPower, claim.QualityAdjPower)
		}
		return nil
	})
	acc.RequireNoError(err, "error iterating power claims")

	acc.Require(committedRawPower.Equals(st.TotalBytesCommitted),
		"sum of raw power in claims %v does not match recorded bytes committed %v",
		committedRawPower, st.TotalBytesCommitted)
	acc.Require(committedQAPower.Equals(st.TotalQABytesCommitted),
		"sum of qa power in claims %v does not match recorded qa power committed %v",
		committedQAPower, st.TotalQABytesCommitted)

	acc.Require(claimsWithSufficientPowerCount == st.MinerAboveMinPowerCount,
		"claims with sufficient power %d does not match MinerAboveMinPowerCount %d",
		claimsWithSufficientPowerCount, st.MinerAboveMinPowerCount)

	acc.Require(st.TotalRawBytePower.Equals(rawPower),
		"recorded raw power %v does not match raw power in claims %v", st.TotalRawBytePower, rawPower)
	acc.Require(st.TotalQualityAdjPower.Equals(qaPower),
		"recorded qa power %v does not match qa power in claims %v", st.TotalQualityAdjPower, qaPower)

	return byAddress
}

func CheckProofValidationInvariants(st *State, store adt.Store, claims ClaimsByAddress, acc *builtin.MessageAccumulator) ProofsByAddress {
	if st.ProofValidationBatch == nil {
		return nil
	}

	proofs := make(ProofsByAddress)
	if queue, err := adt.AsMultimap(store, *st.ProofValidationBatch); err != nil {
		acc.Addf("error loading proof validation queue: %v", err)
	} else {
		err = queue.ForAll(func(key string, arr *adt.Array) error {
			addr, err := address.NewFromBytes([]byte(key))
			if err != nil {
				return err
			}

			claim, found := claims[addr]
			acc.Require(found, "miner %v has proofs awaiting validation but no claim", addr)
			if !found {
				return nil
			}

			var info proof.SealVerifyInfo
			err = arr.ForEach(&info, func(i int64) error {
				acc.Require(claim.SealProofType == info.SealProof, "miner submitted proof with proof type %d different from claim %d",
					info.SealProof, claim.SealProofType)
				proofs[addr] = append(proofs[addr], info)
				return nil
			})
			if err != nil {
				return err
			}
			acc.Require(len(proofs[addr]) <= MaxMinerProveCommitsPerEpoch,
				"miner %v has submitted too many proofs (%d) for batch verification", addr, len(proofs[addr]))
			return nil
		})
		acc.RequireNoError(err, "error iterating proof validation queue")
	}
	return proofs
}
