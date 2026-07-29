/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txmgr

import (
	"sort"

	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger"
	stateconsistency "github.com/hyperledger/fabric/core/ledger/consistency"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statemetadata"
	"github.com/hyperledger/fabric/core/ledger/relaxedstate"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/pkg/errors"
)

// txSimulator is a transaction simulator used in `LockBasedTxMgr`
type txSimulator struct {
	*queryExecutor
	rwsetBuilder              *rwsetutil.RWSetBuilder
	writePerformed            bool
	pvtdataQueriesPerformed   bool
	simulationResultsComputed bool
	paginatedQueriesPerformed bool
	writesetMetadata          ledger.WritesetMetadata
	relaxedWrites             map[string]relaxedstate.Write
}

func newTxSimulator(txmgr *LockBasedTxMgr, txid string, hashFunc rwsetutil.HashFunc) (*txSimulator, error) {
	rwsetBuilder := rwsetutil.NewRWSetBuilder()
	qe := newQueryExecutor(txmgr, txid, rwsetBuilder, true, hashFunc)
	logger.Debugf("constructing new tx simulator txid = [%s]", txid)
	return &txSimulator{
		queryExecutor:    qe,
		rwsetBuilder:     rwsetBuilder,
		writesetMetadata: ledger.WritesetMetadata{},
		relaxedWrites:    map[string]relaxedstate.Write{},
	}, nil
}

// SetState implements method in interface `ledger.TxSimulator`
func (s *txSimulator) SetState(ns string, key string, value []byte) error {
	if err := s.checkWritePrecondition(key, value); err != nil {
		return err
	}
	if level, explicit := s.txmgr.stateConsistency.Resolve(ns, key); explicit && level == stateconsistency.Relaxed {
		s.relaxedWrites[ns+"\x00"+key] = relaxedstate.Write{
			Namespace: ns,
			Key:       key,
			Value:     append([]byte(nil), value...),
			Delete:    value == nil,
		}
		return nil
	}
	s.rwsetBuilder.AddToWriteSet(ns, key, value)
	// if this has a key level signature policy, add it to the interest
	if err := s.checkStateMetadata(ns, key); err != nil {
		return err
	}
	if value == nil {
		return nil
	}
	return s.applyStateConsistency(ns, key)
}

func (s *txSimulator) applyStateConsistency(namespace, key string) error {
	level, explicit := s.txmgr.stateConsistency.Resolve(namespace, key)
	if !explicit {
		return nil
	}
	metadata, err := s.currentStateMetadata(namespace, key)
	if err != nil {
		return err
	}
	metadata, err = stateconsistency.WithLevel(metadata, level)
	if err != nil {
		return err
	}
	s.rwsetBuilder.AddToMetadataWriteSet(namespace, key, metadata)
	return nil
}

func (s *txSimulator) currentStateMetadata(namespace, key string) (map[string][]byte, error) {
	if metadata, pending := s.rwsetBuilder.GetMetadataWriteSet(namespace, key); pending {
		return metadata, nil
	}
	// The consistency write preserves other metadata entries, so it is a
	// read-modify-write operation. Read through the query executor to include
	// the key version in the MVCC read set and avoid losing a concurrent SBE or
	// metadata update.
	return s.queryExecutor.GetStateMetadata(namespace, key)
}

// If this key has a SBE policy, add that policy to the set
func (s *txSimulator) checkStateMetadata(ns string, key string) error {
	metabytes, err := s.txmgr.db.GetStateMetadata(ns, key)
	if err != nil {
		return err
	}
	metadata, err := statemetadata.Deserialize(metabytes)
	if err != nil {
		return err
	}
	s.writesetMetadata.Add(ns, "", key, metadata) // empty string represents the public writeset
	return nil
}

// If this private collection key has a SBE policy, add that policy to the set
func (s *txSimulator) checkPrivateStateMetadata(ns string, coll string, key string) error {
	metabytes, err := s.txmgr.db.GetPrivateDataMetadataByHash(ns, coll, util.ComputeStringHash(key))
	if err != nil {
		return err
	}
	metadata, err := statemetadata.Deserialize(metabytes)
	if err != nil {
		return err
	}
	s.writesetMetadata.Add(ns, coll, key, metadata)
	return nil
}

// DeleteState implements method in interface `ledger.TxSimulator`
func (s *txSimulator) DeleteState(ns string, key string) error {
	return s.SetState(ns, key, nil)
}

// SetStateMultipleKeys implements method in interface `ledger.TxSimulator`
func (s *txSimulator) SetStateMultipleKeys(namespace string, kvs map[string][]byte) error {
	for k, v := range kvs {
		if err := s.SetState(namespace, k, v); err != nil {
			return err
		}
	}
	return nil
}

// SetStateMetadata implements method in interface `ledger.TxSimulator`
func (s *txSimulator) SetStateMetadata(namespace, key string, metadata map[string][]byte) error {
	if err := s.checkWritePrecondition(key, nil); err != nil {
		return err
	}
	if level, explicit := s.txmgr.stateConsistency.Resolve(namespace, key); explicit && level == stateconsistency.Relaxed {
		for metadataKey := range metadata {
			if metadataKey != stateconsistency.MetadataKey {
				return errors.Errorf("metadata %q is not supported for peer-local relaxed state", metadataKey)
			}
		}
		return nil
	}
	if level, ok := metadata[stateconsistency.MetadataKey]; ok {
		if _, err := stateconsistency.ParseLevelBytes(level); err != nil {
			return err
		}
	}
	if level, explicit := s.txmgr.stateConsistency.Resolve(namespace, key); explicit {
		currentMetadata, err := s.currentStateMetadata(namespace, key)
		if err != nil {
			return err
		}
		if metadata == nil {
			currentMetadata = nil
		} else {
			if currentMetadata == nil {
				currentMetadata = map[string][]byte{}
			}
			for metadataKey, metadataValue := range metadata {
				currentMetadata[metadataKey] = append([]byte(nil), metadataValue...)
			}
		}
		metadata, err = stateconsistency.WithLevel(currentMetadata, level)
		if err != nil {
			return err
		}
	}
	s.rwsetBuilder.AddToMetadataWriteSet(namespace, key, metadata)
	return s.checkStateMetadata(namespace, key)
}

// DeleteStateMetadata implements method in interface `ledger.TxSimulator`
func (s *txSimulator) DeleteStateMetadata(namespace, key string) error {
	return s.SetStateMetadata(namespace, key, nil)
}

// SetPrivateData implements method in interface `ledger.TxSimulator`
func (s *txSimulator) SetPrivateData(ns, coll, key string, value []byte) error {
	if err := s.queryExecutor.validateCollName(ns, coll); err != nil {
		return err
	}
	if err := s.checkWritePrecondition(key, value); err != nil {
		return err
	}
	s.writePerformed = true
	s.rwsetBuilder.AddToPvtAndHashedWriteSet(ns, coll, key, value)
	return s.checkPrivateStateMetadata(ns, coll, key)
}

// DeletePrivateData implements method in interface `ledger.TxSimulator`
func (s *txSimulator) DeletePrivateData(ns, coll, key string) error {
	return s.SetPrivateData(ns, coll, key, nil)
}

// PurgePrivateData implements method in interface `ledger.TxSimulator`
func (s *txSimulator) PurgePrivateData(ns, coll, key string) error {
	if err := s.queryExecutor.validateCollName(ns, coll); err != nil {
		return err
	}
	if err := s.checkWritePrecondition(key, nil); err != nil {
		return err
	}
	s.writePerformed = true
	s.rwsetBuilder.AddToPvtAndHashedWriteSetForPurge(ns, coll, key)
	return s.checkPrivateStateMetadata(ns, coll, key)
}

// SetPrivateDataMultipleKeys implements method in interface `ledger.TxSimulator`
func (s *txSimulator) SetPrivateDataMultipleKeys(ns, coll string, kvs map[string][]byte) error {
	for k, v := range kvs {
		if err := s.SetPrivateData(ns, coll, k, v); err != nil {
			return err
		}
	}
	return nil
}

// GetPrivateDataRangeScanIterator implements method in interface `ledger.TxSimulator`
func (s *txSimulator) GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (commonledger.ResultsIterator, error) {
	if err := s.checkBeforePvtdataQueries(); err != nil {
		return nil, err
	}
	return s.queryExecutor.GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey)
}

// SetPrivateDataMetadata implements method in interface `ledger.TxSimulator`
func (s *txSimulator) SetPrivateDataMetadata(namespace, collection, key string, metadata map[string][]byte) error {
	if err := s.queryExecutor.validateCollName(namespace, collection); err != nil {
		return err
	}
	if err := s.checkWritePrecondition(key, nil); err != nil {
		return err
	}
	s.rwsetBuilder.AddToHashedMetadataWriteSet(namespace, collection, key, metadata)
	return s.checkPrivateStateMetadata(namespace, collection, key)
}

// DeletePrivateDataMetadata implements method in interface `ledger.TxSimulator`
func (s *txSimulator) DeletePrivateDataMetadata(namespace, collection, key string) error {
	return s.SetPrivateDataMetadata(namespace, collection, key, nil)
}

// ExecuteQueryOnPrivateData implements method in interface `ledger.TxSimulator`
func (s *txSimulator) ExecuteQueryOnPrivateData(namespace, collection, query string) (commonledger.ResultsIterator, error) {
	if err := s.checkBeforePvtdataQueries(); err != nil {
		return nil, err
	}
	return s.queryExecutor.ExecuteQueryOnPrivateData(namespace, collection, query)
}

// GetStateRangeScanIteratorWithPagination implements method in interface `ledger.QueryExecutor`
func (s *txSimulator) GetStateRangeScanIteratorWithPagination(namespace string, startKey string,
	endKey string, pageSize int32,
) (ledger.QueryResultsIterator, error) {
	if err := s.checkBeforePaginatedQueries(); err != nil {
		return nil, err
	}
	return s.queryExecutor.GetStateRangeScanIteratorWithPagination(namespace, startKey, endKey, pageSize)
}

// ExecuteQueryWithPagination implements method in interface `ledger.QueryExecutor`
func (s *txSimulator) ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (ledger.QueryResultsIterator, error) {
	if err := s.checkBeforePaginatedQueries(); err != nil {
		return nil, err
	}
	return s.queryExecutor.ExecuteQueryWithPagination(namespace, query, bookmark, pageSize)
}

// GetTxSimulationResults implements method in interface `ledger.TxSimulator`
func (s *txSimulator) GetTxSimulationResults() (*ledger.TxSimulationResults, error) {
	if s.simulationResultsComputed {
		return nil, errors.New("this function should only be called once on a transaction simulator instance")
	}
	defer func() { s.simulationResultsComputed = true }()
	logger.Debugf("Simulation completed, getting simulation results")
	if s.queryExecutor.err != nil {
		return nil, s.queryExecutor.err
	}
	s.queryExecutor.addRangeQueryInfo()
	simResults, err := s.rwsetBuilder.GetTxSimulationResults()
	if err != nil {
		return nil, err
	}
	if err := s.txmgr.relaxedState.Stage(s.GrandRelaxedStateSimulation()); err != nil {
		return nil, err
	}
	// The txSimulator structures need to be cloned so that subsequent RW set additions don't modify these TX simulation results
	simResults.PrivateReads = s.privateReads.Clone()
	simResults.WritesetMetadata = s.writesetMetadata.Clone()
	return simResults, nil
}

// GrandRelaxedStateSimulation returns the peer-specific writes excluded from
// Fabric's canonical RWSet. The endorser signs this payload separately.
func (s *txSimulator) GrandRelaxedStateSimulation() *relaxedstate.Simulation {
	reads := make([]relaxedstate.Read, 0, len(s.relaxedReads))
	for _, read := range s.relaxedReads {
		reads = append(reads, read)
	}
	sort.Slice(reads, func(i, j int) bool {
		if reads[i].Namespace == reads[j].Namespace {
			return reads[i].Key < reads[j].Key
		}
		return reads[i].Namespace < reads[j].Namespace
	})
	writes := make([]relaxedstate.Write, 0, len(s.relaxedWrites))
	for _, write := range s.relaxedWrites {
		writes = append(writes, write)
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Namespace == writes[j].Namespace {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Namespace < writes[j].Namespace
	})
	return &relaxedstate.Simulation{
		ChannelID: s.txmgr.ledgerid,
		TxID:      s.txid,
		Reads:     reads,
		Writes:    writes,
	}
}

// StageGrandRelaxedStateSimulation explicitly stages a read-only active-sync
// request. Ordinary read-only proposals are deliberately not staged because
// they may never be submitted as transactions.
func (s *txSimulator) StageGrandRelaxedStateSimulation(simulation *relaxedstate.Simulation) error {
	return s.txmgr.relaxedState.Stage(simulation)
}

// StoreGrandRelaxedStateEvidence persists this peer's individual endorsement
// so V-stage validation can match it to a staged local write or active-sync
// read. Ordinary read-only evidence is returned to the client without being
// persisted because a divergent passive-recovery proposal may never be ordered.
func (s *txSimulator) StoreGrandRelaxedStateEvidence(evidence *relaxedstate.SignedEvidence) error {
	return s.txmgr.relaxedState.StoreEvidence(s.txid, evidence)
}

// ExecuteUpdate implements method in interface `ledger.TxSimulator`
func (s *txSimulator) ExecuteUpdate(query string) error {
	return errors.New("not supported")
}

func (s *txSimulator) checkWritePrecondition(key string, value []byte) error {
	if err := s.checkDone(); err != nil {
		return err
	}
	if err := s.checkPvtdataQueryPerformed(); err != nil {
		return err
	}
	if err := s.checkPaginatedQueryPerformed(); err != nil {
		return err
	}
	s.writePerformed = true
	return s.queryExecutor.txmgr.db.ValidateKeyValue(key, value)
}

func (s *txSimulator) checkBeforePvtdataQueries() error {
	if s.writePerformed {
		return errors.Errorf("txid [%s]: unsuppored transaction. Queries on pvt data is supported only in a read-only transaction", s.txid)
	}
	s.pvtdataQueriesPerformed = true
	return nil
}

func (s *txSimulator) checkPvtdataQueryPerformed() error {
	if s.pvtdataQueriesPerformed {
		return errors.Errorf("txid [%s]: unsuppored transaction. Transaction has already performed queries on pvt data. Writes are not allowed", s.txid)
	}
	return nil
}

func (s *txSimulator) checkBeforePaginatedQueries() error {
	if s.writePerformed {
		return errors.Errorf("txid [%s]: unsuppored transaction. Paginated queries are supported only in a read-only transaction", s.txid)
	}
	s.paginatedQueriesPerformed = true
	return nil
}

func (s *txSimulator) checkPaginatedQueryPerformed() error {
	if s.paginatedQueriesPerformed {
		return errors.Errorf("txid [%s]: unsuppored transaction. Transaction has already performed a paginated query. Writes are not allowed", s.txid)
	}
	return nil
}
