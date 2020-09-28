package market

import (
	"bytes"
	"encoding/binary"
	"sort"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	rtt "github.com/filecoin-project/go-state-types/rt"
	market0 "github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v3/actors/builtin"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v3/actors/runtime"
	"github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

type Actor struct{}

type Runtime = runtime.Runtime

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.AddBalance,
		3:                         a.WithdrawBalance,
		4:                         a.PublishStorageDeals,
		5:                         a.VerifyDealsForActivation,
		6:                         a.ActivateDeals,
		7:                         a.OnMinerSectorsTerminate,
		8:                         a.ComputeDataCommitment,
		9:                         a.CronTick,
	}
}

func (a Actor) Code() cid.Cid {
	return builtin.StorageMarketActorCodeID
}

func (a Actor) IsSingleton() bool {
	return true
}

func (a Actor) State() cbor.Er {
	return new(State)
}

var _ runtime.VMActor = Actor{}

////////////////////////////////////////////////////////////////////////////////
// Actor methods
////////////////////////////////////////////////////////////////////////////////

func (a Actor) Constructor(rt Runtime, _ *abi.EmptyValue) *abi.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	st, err := ConstructState(adt.AsStore(rt))
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to create state")
	rt.StateCreate(st)
	return nil
}

//type WithdrawBalanceParams struct {
//	ProviderOrClientAddress addr.Address
//	Amount                  abi.TokenAmount
//}
type WithdrawBalanceParams = market0.WithdrawBalanceParams

// Attempt to withdraw the specified amount from the balance held in escrow.
// If less than the specified amount is available, yields the entire available balance.
func (a Actor) WithdrawBalance(rt Runtime, params *WithdrawBalanceParams) *abi.EmptyValue {
	if params.Amount.LessThan(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "negative amount %v", params.Amount)
	}

	nominal, recipient, approvedCallers := escrowAddress(rt, params.ProviderOrClientAddress)
	// for providers -> only corresponding owner or worker can withdraw
	// for clients -> only the client i.e the recipient can withdraw
	rt.ValidateImmediateCallerIs(approvedCallers...)

	amountExtracted := abi.NewTokenAmount(0)
	var st State
	rt.StateTransaction(&st, func() {
		msm, err := st.mutator(adt.AsStore(rt)).withEscrowTable(WritePermission).
			withLockedTable(WritePermission).build()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load state")

		// The withdrawable amount might be slightly less than nominal
		// depending on whether or not all relevant entries have been processed
		// by cron
		minBalance, err := msm.lockedTable.Get(nominal)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get locked balance")

		ex, err := msm.escrowTable.SubtractWithMinimum(nominal, params.Amount, minBalance)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to subtract from escrow table")

		err = msm.commitState()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush state")

		amountExtracted = ex
	})

	code := rt.Send(recipient, builtin.MethodSend, nil, amountExtracted, &builtin.Discard{})
	builtin.RequireSuccess(rt, code, "failed to send funds")
	return nil
}

// Deposits the received value into the balance held in escrow.
func (a Actor) AddBalance(rt Runtime, providerOrClientAddress *addr.Address) *abi.EmptyValue {
	msgValue := rt.ValueReceived()
	builtin.RequireParam(rt, msgValue.GreaterThan(big.Zero()), "balance to add must be greater than zero")

	// only signing parties can add balance for client AND provider.
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)

	nominal, _, _ := escrowAddress(rt, *providerOrClientAddress)

	var st State
	rt.StateTransaction(&st, func() {
		msm, err := st.mutator(adt.AsStore(rt)).withEscrowTable(WritePermission).
			withLockedTable(WritePermission).build()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load state")

		err = msm.escrowTable.Add(nominal, msgValue)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to add balance to escrow table")

		err = msm.commitState()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush state")
	})
	return nil
}

//type PublishStorageDealsParams struct {
//	Deals []ClientDealProposal
//}
type PublishStorageDealsParams = market0.PublishStorageDealsParams

//type PublishStorageDealsReturn struct {
//	IDs []abi.DealID
//}
type PublishStorageDealsReturn = market0.PublishStorageDealsReturn

// Publish a new set of storage deals (not yet included in a sector).
func (a Actor) PublishStorageDeals(rt Runtime, params *PublishStorageDealsParams) *PublishStorageDealsReturn {

	// Deal message must have a From field identical to the provider of all the deals.
	// This allows us to retain and verify only the client's signature in each deal proposal itself.
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	if len(params.Deals) == 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "empty deals parameter")
	}

	// All deals should have the same provider so get worker once
	providerRaw := params.Deals[0].Proposal.Provider
	provider, ok := rt.ResolveAddress(providerRaw)
	if !ok {
		rt.Abortf(exitcode.ErrNotFound, "failed to resolve provider address %v", providerRaw)
	}

	codeID, ok := rt.GetActorCodeCID(provider)
	builtin.RequireParam(rt, ok, "no codeId for address %v", provider)
	if !codeID.Equals(builtin.StorageMinerActorCodeID) {
		rt.Abortf(exitcode.ErrIllegalArgument, "deal provider is not a StorageMinerActor")
	}

	caller := rt.Caller()
	_, worker, controllers := builtin.RequestMinerControlAddrs(rt, provider)
	callerOk := caller == worker
	for _, controller := range controllers {
		if callerOk {
			break
		}
		callerOk = caller == controller
	}
	if !callerOk {
		rt.Abortf(exitcode.ErrForbidden, "caller %v is not worker or control address of provider %v", caller, provider)
	}

	resolvedAddrs := make(map[addr.Address]addr.Address, len(params.Deals))
	baselinePower := requestCurrentBaselinePower(rt)
	networkRawPower, networkQAPower := requestCurrentNetworkPower(rt)

	var newDealIds []abi.DealID
	var st State
	rt.StateTransaction(&st, func() {
		msm, err := st.mutator(adt.AsStore(rt)).withPendingProposals(WritePermission).
			withDealProposals(WritePermission).withDealsByEpoch(WritePermission).withEscrowTable(WritePermission).
			withLockedTable(WritePermission).build()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load state")

		// All storage dealProposals will be added in an atomic transaction; this operation will be unrolled if any of them fails.
		for di, deal := range params.Deals {
			validateDeal(rt, deal, networkRawPower, networkQAPower, baselinePower)

			if deal.Proposal.Provider != provider && deal.Proposal.Provider != providerRaw {
				rt.Abortf(exitcode.ErrIllegalArgument, "cannot publish deals from different providers at the same time")
			}

			client, ok := rt.ResolveAddress(deal.Proposal.Client)
			if !ok {
				rt.Abortf(exitcode.ErrNotFound, "failed to resolve client address %v", deal.Proposal.Client)
			}
			// Normalise provider and client addresses in the proposal stored on chain (after signature verification).
			deal.Proposal.Provider = provider
			resolvedAddrs[deal.Proposal.Client] = client
			deal.Proposal.Client = client

			err := msm.lockClientAndProviderBalances(&deal.Proposal)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to lock balance")

			id := msm.generateStorageDealID()

			pcid, err := deal.Proposal.Cid()
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed to take cid of proposal %d", di)

			has, err := msm.pendingDeals.Has(abi.CidKey(pcid))
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check for existence of deal proposal")
			if has {
				rt.Abortf(exitcode.ErrIllegalArgument, "cannot publish duplicate deals")
			}

			err = msm.pendingDeals.Put(abi.CidKey(pcid))
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to set pending deal")

			err = msm.dealProposals.Set(id, &deal.Proposal)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to set deal")

			// We should randomize the first epoch for when the deal will be processed so an attacker isn't able to
			// schedule too many deals for the same tick.
			processEpoch, err := genRandNextEpoch(rt.CurrEpoch(), &deal.Proposal, rt.GetRandomnessFromBeacon)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to generate random process epoch")

			err = msm.dealsByEpoch.Put(processEpoch, id)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to set deal ops by epoch")

			newDealIds = append(newDealIds, id)
		}

		err = msm.commitState()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush state")
	})

	for _, deal := range params.Deals {
		// Check VerifiedClient allowed cap and deduct PieceSize from cap.
		// Either the DealSize is within the available DataCap of the VerifiedClient
		// or this message will fail. We do not allow a deal that is partially verified.
		if deal.Proposal.VerifiedDeal {
			resolvedClient, ok := resolvedAddrs[deal.Proposal.Client]
			builtin.RequireParam(rt, ok, "could not get resolvedClient client address")

			code := rt.Send(
				builtin.VerifiedRegistryActorAddr,
				builtin.MethodsVerifiedRegistry.UseBytes,
				&verifreg.UseBytesParams{
					Address:  resolvedClient,
					DealSize: big.NewIntUnsigned(uint64(deal.Proposal.PieceSize)),
				},
				abi.NewTokenAmount(0),
				&builtin.Discard{},
			)
			builtin.RequireSuccess(rt, code, "failed to add verified deal for client: %v", deal.Proposal.Client)
		}
	}

	return &PublishStorageDealsReturn{IDs: newDealIds}
}

// Changed since v2:
// - Array of sectors rather than just one
// - Removed SectorStart (which is unknown at call time)
type VerifyDealsForActivationParams struct {
	Sectors []SectorDeals
}

type SectorDeals struct {
	SectorExpiry abi.ChainEpoch
	DealIDs      []abi.DealID
}

// Changed since v2:
// - Array of sectors weights
type VerifyDealsForActivationReturn struct {
	Sectors []SectorWeights
}

type SectorWeights struct {
	DealSpace          uint64         // Total space in bytes of submitted deals.
	DealWeight         abi.DealWeight // Total space*time of submitted deals.
	VerifiedDealWeight abi.DealWeight // Total space*time of submitted verified deals.
}

// Computes the weight of deals proposed for inclusion in a number of sectors.
// Deal weight is defined as the sum, over all deals in the set, of the product of deal size and duration.
//
// This method performs some light validation on the way in order to fail early if deals can be
// determined to be invalid for the proposed sector properties.
// Full deal validation is deferred to deal activation since it depends on the activation epoch.
func (a Actor) VerifyDealsForActivation(rt Runtime, params *VerifyDealsForActivationParams) *VerifyDealsForActivationReturn {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Caller()
	currEpoch := rt.CurrEpoch()

	var st State
	rt.StateReadonly(&st)
	store := adt.AsStore(rt)

	proposals, err := AsDealProposalArray(store, st.Proposals)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deal proposals")

	weights := make([]SectorWeights, len(params.Sectors))
	for i, sector := range params.Sectors {
		// Pass the current epoch as the activation epoch for validation.
		// The sector activation epoch isn't yet known, but it's still more helpful to fail now if the deal
		// is so late that a sector activating now couldn't include it.
		dealWeight, verifiedWeight, dealSpace, err := validateAndComputeDealWeight(proposals, sector.DealIDs, minerAddr, sector.SectorExpiry, currEpoch)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to validate deal proposals for activation")

		weights[i] = SectorWeights{
			DealSpace:          dealSpace,
			DealWeight:         dealWeight,
			VerifiedDealWeight: verifiedWeight,
		}
	}

	return &VerifyDealsForActivationReturn{
		Sectors: weights,
	}
}

//type ActivateDealsParams struct {
//	DealIDs      []abi.DealID
//	SectorExpiry abi.ChainEpoch
//}
type ActivateDealsParams = market0.ActivateDealsParams

// Verify that a given set of storage deals is valid for a sector currently being ProveCommitted,
// update the market's internal state accordingly.
func (a Actor) ActivateDeals(rt Runtime, params *ActivateDealsParams) *abi.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Caller()
	currEpoch := rt.CurrEpoch()

	var st State
	store := adt.AsStore(rt)

	// Update deal dealStates.
	rt.StateTransaction(&st, func() {
		_, _, _, err := ValidateDealsForActivation(&st, store, params.DealIDs, minerAddr, params.SectorExpiry, currEpoch)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to validate dealProposals for activation")

		msm, err := st.mutator(adt.AsStore(rt)).withDealStates(WritePermission).
			withPendingProposals(ReadOnlyPermission).withDealProposals(ReadOnlyPermission).build()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load state")

		for _, dealID := range params.DealIDs {
			// This construction could be replaced with a single "update deal state" state method, possibly batched
			// over all deal ids at once.
			_, found, err := msm.dealStates.Get(dealID)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get state for dealId %d", dealID)
			if found {
				rt.Abortf(exitcode.ErrIllegalArgument, "deal %d already included in another sector", dealID)
			}

			proposal, err := getDealProposal(msm.dealProposals, dealID)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get dealId %d", dealID)

			propc, err := proposal.Cid()
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to calculate proposal CID")

			has, err := msm.pendingDeals.Has(abi.CidKey(propc))
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get pending proposal %v", propc)

			if !has {
				rt.Abortf(exitcode.ErrIllegalState, "tried to activate deal that was not in the pending set (%s)", propc)
			}

			err = msm.dealStates.Set(dealID, &DealState{
				SectorStartEpoch: currEpoch,
				LastUpdatedEpoch: epochUndefined,
				SlashEpoch:       epochUndefined,
			})
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to set deal state %d", dealID)
		}

		err = msm.commitState()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush state")
	})

	return nil
}

//type ComputeDataCommitmentParams struct {
//	DealIDs    []abi.DealID
//	SectorType abi.RegisteredSealProof
//}
type ComputeDataCommitmentParams = market0.ComputeDataCommitmentParams

func (a Actor) ComputeDataCommitment(rt Runtime, params *ComputeDataCommitmentParams) *cbg.CborCid {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)

	var st State
	rt.StateReadonly(&st)
	proposals, err := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deal dealProposals")

	pieces := make([]abi.PieceInfo, 0)
	for _, dealID := range params.DealIDs {
		deal, err := getDealProposal(proposals, dealID)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get dealId %d", dealID)

		pieces = append(pieces, abi.PieceInfo{
			PieceCID: deal.PieceCID,
			Size:     deal.PieceSize,
		})
	}

	commd, err := rt.ComputeUnsealedSectorCID(params.SectorType, pieces)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to compute unsealed sector CID: %s", err)
	}

	return (*cbg.CborCid)(&commd)
}

//type OnMinerSectorsTerminateParams struct {
//	Epoch   abi.ChainEpoch
//	DealIDs []abi.DealID
//}
type OnMinerSectorsTerminateParams = market0.OnMinerSectorsTerminateParams

// Terminate a set of deals in response to their containing sector being terminated.
// Slash provider collateral, refund client collateral, and refund partial unpaid escrow
// amount to client.
func (a Actor) OnMinerSectorsTerminate(rt Runtime, params *OnMinerSectorsTerminateParams) *abi.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Caller()

	var st State
	rt.StateTransaction(&st, func() {
		msm, err := st.mutator(adt.AsStore(rt)).withDealStates(WritePermission).
			withDealProposals(ReadOnlyPermission).build()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deal state")

		for _, dealID := range params.DealIDs {
			deal, found, err := msm.dealProposals.Get(dealID)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get deal proposal %v", dealID)
			// deal could have terminated and hence deleted before the sector is terminated.
			// we should simply continue instead of aborting execution here if a deal is not found.
			if !found {
				continue
			}
			builtin.RequireState(rt, deal.Provider == minerAddr, "caller %v is not the provider %v of deal %v",
				minerAddr, deal.Provider, dealID)

			// do not slash expired deals
			if deal.EndEpoch <= params.Epoch {
				continue
			}

			state, found, err := msm.dealStates.Get(dealID)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get deal state %v", dealID)
			if !found {
				rt.Abortf(exitcode.ErrIllegalArgument, "no state for deal %v", dealID)
			}

			// if a deal is already slashed, we don't need to do anything here.
			if state.SlashEpoch != epochUndefined {
				continue
			}

			// mark the deal for slashing here.
			// actual releasing of locked funds for the client and slashing of provider collateral happens in CronTick.
			state.SlashEpoch = params.Epoch

			err = msm.dealStates.Set(dealID, state)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to set deal state %v", dealID)
		}

		err = msm.commitState()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush state")
	})
	return nil
}

func (a Actor) CronTick(rt Runtime, _ *abi.EmptyValue) *abi.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.CronActorAddr)
	amountSlashed := big.Zero()

	var timedOutVerifiedDeals []*DealProposal

	var st State
	rt.StateTransaction(&st, func() {
		updatesNeeded := make(map[abi.ChainEpoch][]abi.DealID)

		msm, err := st.mutator(adt.AsStore(rt)).withDealStates(WritePermission).
			withLockedTable(WritePermission).withEscrowTable(WritePermission).withDealsByEpoch(WritePermission).
			withDealProposals(WritePermission).withPendingProposals(WritePermission).build()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load state")

		for i := st.LastCron + 1; i <= rt.CurrEpoch(); i++ {
			err = msm.dealsByEpoch.ForEach(i, func(dealID abi.DealID) error {
				deal, err := getDealProposal(msm.dealProposals, dealID)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get dealId %d", dealID)

				dcid, err := deal.Cid()
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to calculate CID for proposal %v", dealID)

				state, found, err := msm.dealStates.Get(dealID)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get deal state")

				// deal has been published but not activated yet -> terminate it as it has timed out
				if !found {
					// Not yet appeared in proven sector; check for timeout.
					builtin.RequireState(rt, rt.CurrEpoch() >= deal.StartEpoch, "deal %d processed before start epoch %d",
						dealID, deal.StartEpoch)

					slashed := msm.processDealInitTimedOut(rt, deal)
					if !slashed.IsZero() {
						amountSlashed = big.Add(amountSlashed, slashed)
					}
					if deal.VerifiedDeal {
						timedOutVerifiedDeals = append(timedOutVerifiedDeals, deal)
					}

					// we should not attempt to delete the DealState because it does NOT exist
					if err := deleteDealProposalAndState(dealID, msm.dealStates, msm.dealProposals, true, false); err != nil {
						builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to delete deal %d", dealID)
					}

					pdErr := msm.pendingDeals.Delete(abi.CidKey(dcid))
					builtin.RequireNoErr(rt, pdErr, exitcode.ErrIllegalState, "failed to delete pending proposal %v", dcid)
					return nil
				}

				// if this is the first cron tick for the deal, it should be in the pending state.
				if state.LastUpdatedEpoch == epochUndefined {
					pdErr := msm.pendingDeals.Delete(abi.CidKey(dcid))
					builtin.RequireNoErr(rt, pdErr, exitcode.ErrIllegalState, "failed to delete pending proposal %v", dcid)
				}

				slashAmount, nextEpoch, removeDeal := msm.updatePendingDealState(rt, state, deal, rt.CurrEpoch())
				builtin.RequireState(rt, slashAmount.GreaterThanEqual(big.Zero()), "computed negative slash amount %v for deal %d", slashAmount, dealID)

				if removeDeal {
					builtin.RequireState(rt, nextEpoch == epochUndefined, "removed deal %d should have no scheduled epoch (got %d)", dealID, nextEpoch)

					amountSlashed = big.Add(amountSlashed, slashAmount)
					err := deleteDealProposalAndState(dealID, msm.dealStates, msm.dealProposals, true, true)
					builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to delete deal proposal and states")
				} else {
					builtin.RequireState(rt, nextEpoch > rt.CurrEpoch(), "continuing deal %d next epoch %d should be in future", dealID, nextEpoch)
					builtin.RequireState(rt, slashAmount.IsZero(), "continuing deal %d should not be slashed", dealID)

					// Update deal's LastUpdatedEpoch in DealStates
					state.LastUpdatedEpoch = rt.CurrEpoch()
					err = msm.dealStates.Set(dealID, state)
					builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to set deal state")

					updatesNeeded[nextEpoch] = append(updatesNeeded[nextEpoch], dealID)
				}

				return nil
			})
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to iterate deal ops")

			err = msm.dealsByEpoch.RemoveAll(i)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to delete deal ops for epoch %v", i)
		}

		// Iterate changes in sorted order to ensure that loads/stores
		// are deterministic. Otherwise, we could end up charging an
		// inconsistent amount of gas.
		changedEpochs := make([]abi.ChainEpoch, 0, len(updatesNeeded))
		for epoch := range updatesNeeded { //nolint:nomaprange
			changedEpochs = append(changedEpochs, epoch)
		}

		sort.Slice(changedEpochs, func(i, j int) bool { return changedEpochs[i] < changedEpochs[j] })

		for _, epoch := range changedEpochs {
			err = msm.dealsByEpoch.PutMany(epoch, updatesNeeded[epoch])
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to reinsert deal IDs for epoch %v", epoch)
		}

		st.LastCron = rt.CurrEpoch()

		err = msm.commitState()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush state")
	})

	for _, d := range timedOutVerifiedDeals {
		code := rt.Send(
			builtin.VerifiedRegistryActorAddr,
			builtin.MethodsVerifiedRegistry.RestoreBytes,
			&verifreg.RestoreBytesParams{
				Address:  d.Client,
				DealSize: big.NewIntUnsigned(uint64(d.PieceSize)),
			},
			abi.NewTokenAmount(0),
			&builtin.Discard{},
		)

		if !code.IsSuccess() {
			rt.Log(rtt.ERROR, "failed to send RestoreBytes call to the VerifReg actor for timed-out verified deal, client: %s, dealSize: %v, "+
				"provider: %v, got code %v", d.Client, d.PieceSize, d.Provider, code)
		}
	}

	if !amountSlashed.IsZero() {
		e := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashed, &builtin.Discard{})
		builtin.RequireSuccess(rt, e, "expected send to burnt funds actor to succeed")
	}

	return nil
}

func genRandNextEpoch(currEpoch abi.ChainEpoch, deal *DealProposal, rbF func(crypto.DomainSeparationTag, abi.ChainEpoch, []byte) abi.Randomness) (abi.ChainEpoch, error) {
	buf := bytes.Buffer{}
	if err := deal.MarshalCBOR(&buf); err != nil {
		return epochUndefined, xerrors.Errorf("failed to marshal proposal: %w", err)
	}

	rb := rbF(crypto.DomainSeparationTag_MarketDealCronSeed, currEpoch-1, buf.Bytes())

	// generate a random epoch in [baseEpoch, baseEpoch + DealUpdatesInterval)
	offset := binary.BigEndian.Uint64(rb)

	return deal.StartEpoch + abi.ChainEpoch(offset%uint64(DealUpdatesInterval)), nil
}

func deleteDealProposalAndState(dealId abi.DealID, states *DealMetaArray, proposals *DealArray, removeProposal bool,
	removeState bool) error {
	if removeProposal {
		if err := proposals.Delete(dealId); err != nil {
			return xerrors.Errorf("failed to delete proposal %d : %w", dealId, err)
		}
	}

	if removeState {
		if err := states.Delete(dealId); err != nil {
			return xerrors.Errorf("failed to delete deal state: %w", err)
		}
	}

	return nil
}

//
// Exported functions
//

// Validates a collection of deal dealProposals for activation, and returns their combined weight,
// split into regular deal weight and verified deal weight.
func ValidateDealsForActivation(
	st *State, store adt.Store, dealIDs []abi.DealID, minerAddr addr.Address, sectorExpiry, currEpoch abi.ChainEpoch,
) (big.Int, big.Int, uint64, error) {
	proposals, err := AsDealProposalArray(store, st.Proposals)
	if err != nil {
		return big.Int{}, big.Int{}, 0, xerrors.Errorf("failed to load dealProposals: %w", err)
	}

	return validateAndComputeDealWeight(proposals, dealIDs, minerAddr, sectorExpiry, currEpoch)
}

////////////////////////////////////////////////////////////////////////////////
// Checks
////////////////////////////////////////////////////////////////////////////////

func validateAndComputeDealWeight(proposals *DealArray, dealIDs []abi.DealID, minerAddr addr.Address,
	sectorExpiry abi.ChainEpoch, sectorActivation abi.ChainEpoch) (big.Int, big.Int, uint64, error) {

	seenDealIDs := make(map[abi.DealID]struct{}, len(dealIDs))
	totalDealSpace := uint64(0)
	totalDealSpaceTime := big.Zero()
	totalVerifiedSpaceTime := big.Zero()
	for _, dealID := range dealIDs {
		// Make sure we don't double-count deals.
		if _, seen := seenDealIDs[dealID]; seen {
			return big.Int{}, big.Int{}, 0, exitcode.ErrIllegalArgument.Wrapf("deal ID %d present multiple times", dealID)
		}
		seenDealIDs[dealID] = struct{}{}

		proposal, found, err := proposals.Get(dealID)
		if err != nil {
			return big.Int{}, big.Int{}, 0, xerrors.Errorf("failed to load deal %d: %w", dealID, err)
		}
		if !found {
			return big.Int{}, big.Int{}, 0, exitcode.ErrNotFound.Wrapf("no such deal %d", dealID)
		}
		if err = validateDealCanActivate(proposal, minerAddr, sectorExpiry, sectorActivation); err != nil {
			return big.Int{}, big.Int{}, 0, xerrors.Errorf("cannot activate deal %d: %w", dealID, err)
		}

		// Compute deal weight
		totalDealSpace += uint64(proposal.PieceSize)
		dealSpaceTime := DealWeight(proposal)
		if proposal.VerifiedDeal {
			totalVerifiedSpaceTime = big.Add(totalVerifiedSpaceTime, dealSpaceTime)
		} else {
			totalDealSpaceTime = big.Add(totalDealSpaceTime, dealSpaceTime)
		}
	}
	return totalDealSpaceTime, totalVerifiedSpaceTime, totalDealSpace, nil
}

func validateDealCanActivate(proposal *DealProposal, minerAddr addr.Address, sectorExpiration, sectorActivation abi.ChainEpoch) error {
	if proposal.Provider != minerAddr {
		return exitcode.ErrForbidden.Wrapf("proposal has provider %v, must be %v", proposal.Provider, minerAddr)
	}
	if sectorActivation > proposal.StartEpoch {
		return exitcode.ErrIllegalArgument.Wrapf("proposal start epoch %d has already elapsed at %d", proposal.StartEpoch, sectorActivation)
	}
	if proposal.EndEpoch > sectorExpiration {
		return exitcode.ErrIllegalArgument.Wrapf("proposal expiration %d exceeds sector expiration %d", proposal.EndEpoch, sectorExpiration)
	}
	return nil
}

func validateDeal(rt Runtime, deal ClientDealProposal, networkRawPower, networkQAPower, baselinePower abi.StoragePower) {
	if err := dealProposalIsInternallyValid(rt, deal); err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "Invalid deal proposal: %s", err)
	}

	proposal := deal.Proposal

	if len(proposal.Label) > DealMaxLabelSize {
		rt.Abortf(exitcode.ErrIllegalArgument, "deal label can be at most %d bytes, is %d", DealMaxLabelSize, len(proposal.Label))
	}

	if err := proposal.PieceSize.Validate(); err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "proposal piece size is invalid: %v", err)
	}

	if !proposal.PieceCID.Defined() {
		rt.Abortf(exitcode.ErrIllegalArgument, "proposal PieceCID undefined")
	}

	if proposal.PieceCID.Prefix() != PieceCIDPrefix {
		rt.Abortf(exitcode.ErrIllegalArgument, "proposal PieceCID had wrong prefix")
	}

	if proposal.EndEpoch <= proposal.StartEpoch {
		rt.Abortf(exitcode.ErrIllegalArgument, "proposal end before proposal start")
	}

	if rt.CurrEpoch() > proposal.StartEpoch {
		rt.Abortf(exitcode.ErrIllegalArgument, "Deal start epoch has already elapsed.")
	}

	minDuration, maxDuration := DealDurationBounds(proposal.PieceSize)
	if proposal.Duration() < minDuration || proposal.Duration() > maxDuration {
		rt.Abortf(exitcode.ErrIllegalArgument, "Deal duration out of bounds.")
	}

	minPrice, maxPrice := DealPricePerEpochBounds(proposal.PieceSize, proposal.Duration())
	if proposal.StoragePricePerEpoch.LessThan(minPrice) || proposal.StoragePricePerEpoch.GreaterThan(maxPrice) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Storage price out of bounds.")
	}

	minProviderCollateral, maxProviderCollateral := DealProviderCollateralBounds(proposal.PieceSize, proposal.VerifiedDeal,
		networkRawPower, networkQAPower, baselinePower, rt.TotalFilCircSupply())
	if proposal.ProviderCollateral.LessThan(minProviderCollateral) || proposal.ProviderCollateral.GreaterThan(maxProviderCollateral) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Provider collateral out of bounds.")
	}

	minClientCollateral, maxClientCollateral := DealClientCollateralBounds(proposal.PieceSize, proposal.Duration())
	if proposal.ClientCollateral.LessThan(minClientCollateral) || proposal.ClientCollateral.GreaterThan(maxClientCollateral) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Client collateral out of bounds.")
	}
}

//
// Helpers
//

// Resolves a provider or client address to the canonical form against which a balance should be held, and
// the designated recipient address of withdrawals (which is the same, for simple account parties).
func escrowAddress(rt Runtime, address addr.Address) (nominal addr.Address, recipient addr.Address, approved []addr.Address) {
	// Resolve the provided address to the canonical form against which the balance is held.
	nominal, ok := rt.ResolveAddress(address)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to resolve address %v", address)
	}

	codeID, ok := rt.GetActorCodeCID(nominal)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "no code for address %v", nominal)
	}

	if codeID.Equals(builtin.StorageMinerActorCodeID) {
		// Storage miner actor entry; implied funds recipient is the associated owner address.
		ownerAddr, workerAddr, _ := builtin.RequestMinerControlAddrs(rt, nominal)
		return nominal, ownerAddr, []addr.Address{ownerAddr, workerAddr}
	}

	return nominal, nominal, []addr.Address{nominal}
}

func getDealProposal(proposals *DealArray, dealID abi.DealID) (*DealProposal, error) {
	proposal, found, err := proposals.Get(dealID)
	if err != nil {
		return nil, xerrors.Errorf("failed to load proposal: %w", err)
	}
	if !found {
		return nil, exitcode.ErrNotFound.Wrapf("no such deal %d", dealID)
	}

	return proposal, nil
}

// Requests the current epoch target block reward from the reward actor.
func requestCurrentBaselinePower(rt Runtime) abi.StoragePower {
	var ret reward.ThisEpochRewardReturn
	code := rt.Send(builtin.RewardActorAddr, builtin.MethodsReward.ThisEpochReward, nil, big.Zero(), &ret)
	builtin.RequireSuccess(rt, code, "failed to check epoch baseline power")
	return ret.ThisEpochBaselinePower
}

// Requests the current network total power and pledge from the power actor.
func requestCurrentNetworkPower(rt Runtime) (rawPower, qaPower abi.StoragePower) {
	var pwr power.CurrentTotalPowerReturn
	code := rt.Send(builtin.StoragePowerActorAddr, builtin.MethodsPower.CurrentTotalPower, nil, big.Zero(), &pwr)
	builtin.RequireSuccess(rt, code, "failed to check current power")
	return pwr.RawBytePower, pwr.QualityAdjPower
}
