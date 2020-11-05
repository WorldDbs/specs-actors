
# Actors severe mainnet bugs post-mortem – Jan 2021

[**Intro**](#intro)

[**Consensus Fault Author Check**](#A)
	
- [Part 1: Facts](#facts-A)
- [Part 2: Discussion](#disc-A)

[**Non-deterministic Map Iteration**](#B)

- [Part 1: Facts](#facts-B)
- [Part 2: Discussion](#disc-B)

[**Multisig Infinite Re-entrancy**](#C)

- [Part 1: Facts](#facts-C)
- [Part 2: Discussion](#disc-C)

[**Summary discussion and recommendations**](#recc)

- [What *has* been working for us?](#working)



# Intro
<a name="intro"></a>

As of January 2021, the actors team is aware of two network-critical issues that have been released into Filecoin mainnet, and one further that turned out to be dangerous but not critical. This document describes the issues and circumstances around their development, and contains notes of discussion about how we might change work practices to reduce the likelihood or impact of other such issues.



*   Consensus fault block author not checked
*   Non-deterministic Go map iteration
*   Multisig infinite re-entrancy

Some other, less-critical issues, have been uncovered too, but are not explored here.

The Filecoin Space Race test network transitioned into mainnet on 15 October without a network reset. Shortly prior to that, an on-chain network upgrade upgraded the actors code to release [2.0.3](https://github.com/filecoin-project/specs-actors/releases/tag/v2.0.3) ([code](https://github.com/filecoin-project/specs-actors/tree/v2.0.3)). The prior v0.9 release chain had been considered as pre-mainnet, so this document takes releases 2.0.3 and subsequent as being intended for mainnet.


# Consensus Fault Author Check
<a name="A"></a>

## Part 1: Facts
<a name="facts-A"></a>

### Description of the bug

The ReportConsensusFault method of the storage miner actor ([code@v2.0.3](https://github.com/filecoin-project/specs-actors/blob/v2.0.3/actors/builtin/miner/miner_actor.go#L1525-L1594)) is intended to be called by any network participant to report a consensus fault committed by the miner. The reporter provides the data of two or three block headers (depending on the fault “type”). The method passes the headers to a runtime syscall VerifyConsensusFault ([code](https://github.com/filecoin-project/specs-actors/blob/v2.0.3/actors/runtime/runtime.go#L187-L196)) which verifies that the headers represent a valid fault, and then applies penalties to the receiving miner and a small reward to the submitter. Penalties include a fee and ineligibility to produce blocks for the next 900 epochs.

The syscall returns a tuple containing the address of the miner at fault, the epoch of the highest of the blocks comprising it, and the type of fault (or an error, if the fault is not valid). The ReportConsensusFault method **failed to check that the address of the miner at fault matched the receiving actor’s address**, i.e. that the correct miner was being reported. Because of this, any consensus fault specimen could be used to penalize any and all miners on the network. In the extreme case, this could have led to all miners being unable to produce blocks, and a hard-to-recover chain halt.


### Discovery and mitigation

Community contributor [@zgfzgf](https://github.com/zgfzgf) reported the issue to us on 23 October 2020. 

There were no instances of this error being exploited. We quietly mitigated the issue in the release of Lotus 1.1.3 Nov 13 2020, by changing the VerifyConsensusFault syscall implementation to require the offending blocks’ signers to be the worker key of the receiving actor. An exploitation at this point would have caused a fork between participants running 1.1.3 and earlier versions. The mitigation was considered to be fully in effect only after Lotus 1.2 was released, which included the network version 7 upgrade (code released Nov 18th 2020 and network upgrade at epoch 265200), and thus forced all participants to be running code containing the workaround.

We implemented the public fix implemented in [#1314](https://github.com/filecoin-project/specs-actors/pull/1314) on 3 December 2020, which will be launched into the network with actors v3 (estimated early March 2021).


### Origins

The ReportConsensusFault method was originally implemented and reviewed in non-executable code in an earlier version of the Filecoin spec as [#789](https://github.com/filecoin-project/specs/pull/789) on 9 Jan 2020 in the storage power actor. This [implementation](https://github.com/filecoin-project/specs/blob/218d29ac6c48c342f41ec001787bc158f35e9a35/src/actors/builtin/storage_power/storage_power_actor_code.go#L193-L251) was incomplete, skipping any mechanism to inspect block headers. As a result of [discussion](https://github.com/filecoin-project/specs/pull/789#discussion_r364113492), the target (“slashee”) address was taken as an external parameter that was not verified as corresponding with evidence. The runtime contained no block-header fault verification method. Spec code had no tests.

Thanks to review we removed this unverified parameter in [#231](https://github.com/filecoin-project/specs-actors/pull/231) on 6 March, after the VerifyConsensusFault syscall existed. In this [implementation](https://github.com/filecoin-project/specs-actors/blob/64ac74fb1cb16077b90715f718a746d55b978deb/actors/builtin/power/power_actor.go#L470-L519), the fault target returned by VerifyConsensusFault identified the offending miner, thus fixing the issue. There were no tests for this method, and #231 didn’t add any.

We reintroduced the bug in [#271](https://github.com/filecoin-project/specs-actors/pull/271) on 11 April 2020 when moving ReportConsensusFault to the miner actor, as part of a restructuring of pledge accounting from the power to miner actors ([ReportConsensusFault@#271](https://github.com/filecoin-project/specs-actors/blob/889f1a6f9f525ccb6a06cc10858e6917e20bae39/actors/builtin/miner/miner_actor.go#L675-L716)). This change removed all usage of the fault target returned by VerifyConsensusFault, inferring the target to be the receiver but failing to check that they match. There were no miner actor tests, and this PR didn’t add any.

We introduced the first test of ReportConsensusFault in [#472](https://github.com/filecoin-project/specs-actors/pull/472) on 25 June. This was a happy path test checking that deals were correctly terminated. Other patchwork tests were added over time as other tweaks implemented. The absence of a desirable check could not subsequently be detected through code coverage metrics.


## Part 2: Discussion
<a name="disc-A"></a>



*   The conditions under which the bug was introduced, and re-introduced, are unlikely to recur again (spec author rather than engineer implementer, non-executable code, no initial or existing tests). Large contributions from new contributors might carry similar risk though.
*   There was no spec for the method behaviour, since the 2019 spec development process had favoured code over words.
    *   When reviewing, auditors treated the method as a black box.
    *   We would expect a spec to have stated this condition explicitly, and so writing either code or tests from such a spec would have caught it.
*   The implicit receiver target is a subtle bug, it’s difficult to detect a missing check.
    *   Simple line coverage metrics cannot detect such omissions
    *   Casual observers might have considered the VerifyConsensusFault syscall to have done the target check.
*   The team were very methodical about testing many actors, and parts of the miner actor, but didn’t reach that level with miner actor behaviour beyond sector lifecycle. 
    *   Time pressure for mainnet launch
    *   The miner actor is too big, daunting complexity
    *   ReportConsensusFault was not part of code that was changing
    *   Miner actor as a whole had lots of tests, effort seemed more profitable elsewhere
*   More critical eyes on the code would have helped, for any reason; testing is one reason
*   Automated and tool-assisted approaches are unlikely to have found this. Mitigation of similar issues in the future depends on design and development process
    *   Fuzz testing could have detected this only if it fuzzed the syscall mock return value, which is very unlikely.
    *   Fuzz testing at Lotus integration level could possibly have detected it, but unlikely since values would need to be valid consensus-faulty block headers with the same signer but not the receiver
    *   Agent-based tests would not have been programmed to try this path
*   **Recommendation**: write a spec (or FIP) in prose ahead of code
    *   This involves more eyes and minds thinking about correct behaviour
    *   A spec provides an additional guide for test coverage
*   **Recommendation**: require tests to be written concurrently with new code
*   **Recommendation**: increase review breadth to get more eyes on each change


# Non-deterministic Map Iteration
<a name="B"></a>

## Part 1: Facts
<a name="facts-B"></a>

### Description of the bug

DeadlineSectorMap and PartitionSectorMap are implemented as Go maps with nondeterministic range order.  These two types sort the underlying data before running to achieve deterministic ranging behavior. There was a bug in the sorting function that caused no actual sorting to take place, leading to non deterministic ranging in the same order as the Go map range.

Four code paths exercised this ranging behavior on both the deadline and partition sectors maps.



1. ProveCommitCallback
2. TerminateSectors
3. DeclareFaults
4. DeclareFaultsRecovered

For (1) this code path is only hit on CC upgrade. Nondeterminism can only show up when two CC upgrades are scheduled by the same miner in one epoch. The error can occur in (2) when multiple sectors are terminated at once. For (3) or (4) the error could have been triggered by a single DeclareFaults message declaring more than one partition faulty/recovered. 

(1) is rarely exercised because CC upgrades are rare on mainnet and the bug would require one miner to do enough CC upgrades to get two within the same epoch. (3) is economically deprecated, since as of v2 actors it has been cheaper to implicitly declare faults with a missed post. 


### Discovery and mitigation

Code path (2) terminating multiple sectors led to a chain head mismatch on December 19th with different orderings resulting in different gas values used.  This caused a disruptive chain halt. We fixed bug in actors v2.3.3, v2 change here: [#1334](https://github.com/filecoin-project/specs-actors/pull/1334) and clearer v3 forward port: [#1335](https://github.com/filecoin-project/specs-actors/pull/1335).  These changes were consumed into lotus and released in version 1.4.0. 


### Origins

We introduced the bug when refactoring deadline state methods in [#761](https://github.com/filecoin-project/specs-actors/pull/761). We then propagated it into the PR introducing the DeadlineSectorMap and PartitionSectorMap in [#861](https://github.com/filecoin-project/specs-actors/pull/861). This PR includes tests of these abstractions, but they do not cover ranging non-empty collections. There also were no tests checking determinism over non-empty collections, or any existing testing patterns of this sort to build off of.

We made no significant changes to these abstractions or tests until bug discovery.


## Part 2: Discussion
<a name="disc-B"></a>


*   We didn’t discover this one until it affected the network
*   Lotus had not previously supported terminating sectors
*   Error only apparent when terminating _multiple_ deadlines or partitions
*   Non-determinism, including unstable gas usage, is not tested in actors directly
    *   It is checked in the VM test vectors, but they target the VM itself, and do not cover a wide range of actor behaviour
*   We could use a “golden file” approach to check for deterministic execution
    *   This would also be helpful for detecting unintended behaviour changes between network version upgrades.
*   Mutation testing could have detected this in theory, since the “sorting” line has no effect.
    *   We’d need to customize a mutation testing library to work well with actors.
*   The deadline_state_test.go does exercise termination of multiple sectors. This might have caught the issue in combination with a golden file.
*   Simple fuzz testing isn’t helpful; the bug required building up enough state to terminate multiple sectors. 
*   Randomisation over high level agent behaviour could have exercised this (in combination with a golden/expectation file).
*   We’re not aware of anything other than map iteration that could be non-deterministic, but something could be introduced later.
    * We explicitly lint against map iteration so introducing map iteration without explicitly indicating an exception will raise a flag. The issue here was that we mae a mistake while implementing a pathway that we explicitly indicated as an exception.
*   **Recommendation**: introduce a golden file (or trace, or other determinism check) to unit, scenario and agent tests.
*   **Recommendation**: expand agent-based testing to range over possible actor behaviours (in combination with golden file).


# Multisig Infinite Re-entrancy
<a name="C"></a>

## Part 1: Facts
<a name="facts-C"></a>

### Description of the bug

A 1 of n multisig actor could add itself as a signer, propose a message to the multisig actor address such that the proposal message is an approval message to the multisig actor address approving the tx id of the approval message itself. A multisig transaction of this form recurses indefinitely approving the approval message at each iteration.  This was possible because the multisig actor did not remove the tx id from multisig state before launching the send call that executed the transaction.

Furthermore while executing a nested send the node’s vm adds function call stack frames to the go runtime. Go panics with a stack overflow error when the stack gets to about 1GB.  If the send calls looped deep enough this will cause all nodes on the network to panic creating a difficult to recover network halt.

Because there was no send call depth limiting in place during initial network launch, this widespread node pancing was a real concern. However each multisig call spends some gas. For the mainnet protocol configuration the gas limit was exceeded with a only a program stack size of around 250 MB making the stack overflow not feasible.

Since unit tests interact with a mock runtime to fake out all calls to other actors our testing framework was not equipped to exercise this edge case.


### Discovery and mitigation

During manual testing the day before liftoff on Oct 14 2020, we were alerted that a bug occurred during multisig removal such that a multisig signer could not remove itself. This led us to discover the general reentrancy issue and the associated potential for the infinite looping approve transaction.

We monitored mainnet for multisig signers that added themselves as signers post liftoff and later assessed that it was not feasible to use the bug to stack overflow and crash all nodes on Oct 17 2020. Furthermore we analyzied all other actor call paths and determined that there was no path to overflowing the stack while remaining under the gas limit systemwide using reentrant sends. We found a less critical DOS vector on Oct 19 2020 which exploited slow ID-address lookup in the case of many nested VM calls. This attack used the same self-approving multisig, but referenced the multisig's robust address to force an ID lookup every send.

Members of the lotus team landed a fix into lotus removing this DOS vector with a state caching optimization the same day in [#4481](https://github.com/filecoin-project/lotus/pull/4481) released in lotus v1.1.0.

With the threat to harm the network with multisig looping effectively eliminated the lotus team then introduced a VM call stack depth limit of 4096 on Oct 20 2020[ #4506](https://github.com/filecoin-project/lotus/pull/4506).

Recursion up to the stack limit can still be triggered on mainnet. We plan to refactor the multisig actor to remove it in the future.


### Origins

The original spec for the multisig actor [PR #139](https://github.com/filecoin-project/specs/pull/139/files#diff-c8ac5ac03150002fa720d13154770718087f5f480fc72334af18a83582e3d3b2R831) made on Mar 26 2019 does not definitively have this bug because the state storage model is not specified in enough detail. 

The bug was introduced in Lotus on Aug 5th 2019 during the first multisig actor commit [#105](https://github.com/filecoin-project/lotus/pull/105/files).  This case is more subtle than the final bug because the tx.Approved value is both modified and checked before send and would halt looping if state updates persisted between send calls. However the actor state is not written until after send so this state change isn’t seen in subsequent calls allowing looping.

The spec team propagated/introduced a variant of the bug into the spec [here](https://github.com/filecoin-project/specs/pull/774/files#diff-3c41c3cc8a036c552bd599ebd6529f29ecb1764304cbd3aa463487da67602d39R165) in PR [#774](https://github.com/filecoin-project/specs/pull/774) on Dec 22nd 2019. The lotus code appears to have been used as a template for this work.  Interestingly the original lotus issue is resolved in this PR because state is saved before Send. However this PR also removes the lotus check validating that the approver has not previously called this method so the correct state saving placement no longer helps stop looping.

We carried over this bug into the first commit of specs actors ([link](https://github.com/filecoin-project/specs-actors/commit/ac6379d84c1e8d7fac9fada6b15cc010db261e0a#diff-0511acf500959014d9e8b44d971eb00fc87109c11db18b18137408afb374a608R166)) on Jan 8th, 2020.


## Part 2: Discussion
<a name="disc-C"></a>



*   This turned out to be non-critical only because the gas limit restricted stack depth. A gas limit 4x larger would have opened up the attack.
*   The multisig actor was received by the actors team (via spec) as more or less complete, with change discouraged. Apart from adding tests, we did not spend a lot of time with it.
*   The state transaction mechanism inherited from the spec was intended to prevent this general class of issue, but it did not because the multisig does multiple transactions in a single method.
*   The Checks-Effects-Interactions pattern would be an effective defence against this issue.
    *   It’s difficult to fully comply across the whole codebase
*   Unit tests are not sufficient to exercise recursive methods
    *   We didn’t have the VM at the time we tested this actor.
    *   If we had used VM tests, very likely the test author would have considered recursion.
*   None of the actors team were experienced smart contract developers at the time
*   **Recommendation**: implement tests for actors that essentially interact with others as VM tests, exercising the complete interaction
*   **Recommendation**: implement agent-based testing to range over possible actor behaviour, including actors like the multisig and payment channel


# Summary discussion and recommendations
<a name="recc"></a>


*   Golden files are a useful tool for avoiding unintended behavior changes. Generating golden files from test traces and always running tests against these traces.
*   Non-determinism needs to be taken seriously because this behavior is hard to detect. This makes errors more likely to reach mainnet, where they always have a high negative impact.
*   For consensus fault and multisig, interactions with the runtime were a significant factor. It is not always clear that tests are missing coverage on code paths that rely on real runtime behavior. The mock runtime can hide things.
*   “More testing” can’t be the only solution: the miner actor received a lot of testing attention and coverage and was the site of the two most serious bugs
*   A more clear and precise spec that implementations adhered to more strictly would have helped with consensus fault, and possibly multisig too. For example, this would have helped us launch mainnet with a stack depth limit.
*   Behaviour automation with agents could cover a lot of ground that we won’t reach with manual testing. That approach could have found the multisig reentrancy as well as non-determinism (coupled with golden file tracing).
*   None of these errors could be detected by line coverage metrics alone. However some metrics about path coverage might give insights into less exercised actor code. We can get path/value coverage with behaviour automation (agent tests)
*   **Recommendation**: move multisig tests to scenario tests using the testing VM, rely more heavily on the testing VM for new tests.
*   **Recommendation**: more tracing/metrics about coverage of edge cases, not line coverage but, e.g. parameter coverage over methods. For example we should be able to know right away that we test sector termination in a single partition but not in multiple.
*   **Recommendation**: just revising all our tests is possibly a way to draw out some new things. There’s likely cruft that, if cleaned out, could reveal new testing angles.


## What *has* been working for us?
<a name="working"></a>

*   State invariant checks
    *   These have detected errors in code that produce unexpected state.
*   Scenario tests, whenever there are interactions between actors.
    *   Very useful for reproducing errors.
*   External auditing has caught lots of things.
*   Unit test coverage.
*   Very good for verifying the effects of a change in code.