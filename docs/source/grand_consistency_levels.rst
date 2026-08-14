:orphan:

GraND per-state consistency levels
==================================

GraND classifies each public world-state key, identified by its
``(chaincode namespace, key)`` tuple, as ``strong``, ``normal``, or
``relaxed``. Canonical strong/normal classification can be stored in Fabric's
existing state metadata under ``GRAND_CONSISTENCY_LEVEL``. A key explicitly
classified as relaxed by the manifest is instead routed to the peer-local
GraND database; its value and metadata are excluded from the canonical RWSet.
This implementation does not modify the ordering service or Fabric protobufs.

Backward compatibility
----------------------

State without ``GRAND_CONSISTENCY_LEVEL`` is interpreted as ``normal``. An
unmodified Fabric database can therefore be opened without migration. At this
stage, ``normal`` still follows standard Fabric simulation, endorsement,
ordering, MVCC validation, and commit, so changing the compatibility label does
not yet relax execution. Version 1 applies explicit classifications only to
public state covered by a manifest rule; private-data classification is not yet
implemented.

Manifest
--------

Set ``ledger.state.consistency.manifest`` in ``core.yaml`` to an offline or
manually generated YAML manifest. All endorsing peers for a channel must use
the same manifest. The manifest bytes are SHA-256 hashed when loaded, and a
missing, malformed, or unsupported manifest prevents the peer ledger from
opening.

.. code-block:: yaml

   ledger:
     state:
       consistency:
         manifest: /etc/hyperledger/peercfg/grand-consistency.yaml

The equivalent environment variable is
``CORE_LEDGER_STATE_CONSISTENCY_MANIFEST``. For a containerized peer, the file
must be mounted inside the container at the configured path.

.. code-block:: yaml

   version: 1
   defaultLevel: normal
   rules:
     - namespace: basic
       level: normal
     - namespace: basic
       keyPrefix: "normal:"
       level: normal
     - namespace: basic
       key: "relaxed:oracle"
       level: relaxed
       tierThreshold: 3

Rules have deterministic precedence: exact key, longest matching prefix,
namespace rule, and finally the implicit normal default. Manifest version 1
requires a normal default so old and unlisted state has one consistent
classification. Explicit namespace, prefix, and exact-key rules may still
classify selected public state as strong or relaxed.

``tierThreshold`` is optional and is accepted only on relaxed rules. It must be
a positive integer. Omitting it disables tier-triggered preventive
synchronization for states selected by that rule.

Relaxed-state execution
-----------------------

For an explicit manifest rule resolving to ``relaxed``, ordinary chaincode
``GetState`` and ``PutState`` operations are automatically routed to a
persistent peer-local LevelDB. Relaxed reads do not enter the canonical read
set. Relaxed writes are staged under the transaction ID and do not enter the
canonical write or metadata sets. ``GetStateMetadata`` synthesizes the
``GRAND_CONSISTENCY_LEVEL=relaxed`` result from the manifest.

Each endorser separately signs its local pending delta while returning the
same canonical ``ProposalResponsePayload``. The modified client transaction
builder sorts those evidence records and places the bundle in the existing
``ChaincodeProposalPayload.TransientMap`` under the reserved key
``GRAND_RELAXED_EVIDENCE_V1``. The transaction creator signs the bundle. The
committing peer excludes only that reserved field when recomputing the
canonical proposal hash, verifies every local MSP signature, and requires its
own pending delta to match its evidence. A VALID transaction commits the local
delta; an invalid transaction discards it.

Relaxed values, reads, writes, and signed evidence also carry a propagation
``tier``. The first online rule assigns tier zero to a relaxed write without a
relaxed input, and otherwise assigns ``max(input tier) + 1`` to every relaxed
output of the transaction. If a VALID write reaches its fixed manifest
threshold, the peer atomically stores a durable preventive-sync request and
logs ``GRAND_PREVENTIVE_SYNC_REQUIRED``. An external controller can then invoke
the existing certified active-sync primitive. A VALID active-sync replacement
clears the request and resets the value tier to zero. Threshold feedback and
per-path ``tauPlan`` evaluation are not part of this first implementation.

The first implementation supports exact-key ``GetState``,
``GetStateMultipleKeys``, ``PutState``, and delete. Range and rich queries do
not merge the relaxed database. Threshold feedback, promotion, V-stage replay
repair, and a final evidence-root protocol remain future work. In particular,
canonical endorsements do not yet bind the final evidence bundle, so this is
an experimental one-phase vertical slice rather than the final protocol.

Core Go access
--------------

Code holding a ``ledger.QueryExecutor`` or ``ledger.TxSimulator`` can use
``consistency.GetStateLevel`` and ``consistency.SetStateLevel``. The concrete
state DB also exposes ``GetStateConsistencyLevel``, and
``statedb.VersionedValue`` exposes ``StateConsistencyLevel``. These helpers
preserve other state metadata and validate all three canonical values.
