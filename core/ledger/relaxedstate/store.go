/*
Copyright 2026 GraND Authors.

SPDX-License-Identifier: Apache-2.0
*/

// Package relaxedstate implements the peer-local part of GraND's graded world
// state. Relaxed values are staged during endorsement, individually signed by
// the executing peer, and committed only when the corresponding canonical
// Fabric transaction is valid.
package relaxedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	LocalEndorsementDomain   = "GRAND_LOCAL_STATE_ENDORSEMENT_V1"
	StateUpdatePurpose       = "state-update"
	ActiveSyncPurpose        = "active-sync"
	MedianJSONPriceAlgorithm = "median-json-price-v1"
)

type Read struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
}

type Write struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	Delete    bool   `json:"delete,omitempty"`
}

type Simulation struct {
	ChannelID string  `json:"channelId"`
	TxID      string  `json:"txId"`
	Purpose   string  `json:"purpose"`
	Reads     []Read  `json:"reads,omitempty"`
	Writes    []Write `json:"writes,omitempty"`
}

type ReadEvidence struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	ValueHash []byte `json:"valueHash"`
}

type WriteEvidence struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	ValueHash []byte `json:"valueHash"`
	Delete    bool   `json:"delete,omitempty"`
}

type EvidencePayload struct {
	Domain              string          `json:"domain"`
	ChannelID           string          `json:"channelId"`
	TxID                string          `json:"txId"`
	Purpose             string          `json:"purpose"`
	ProposalHash        []byte          `json:"proposalHash"`
	CanonicalResultHash []byte          `json:"canonicalResultHash"`
	Reads               []ReadEvidence  `json:"reads,omitempty"`
	Writes              []WriteEvidence `json:"writes,omitempty"`
}

type SignedEvidence struct {
	Payload   EvidencePayload `json:"payload"`
	Endorser  []byte          `json:"endorser"`
	Signature []byte          `json:"signature"`
}

type ValueRecord struct {
	Namespace   string            `json:"namespace"`
	Key         string            `json:"key"`
	Value       []byte            `json:"value"`
	TxID        string            `json:"txId"`
	BlockNumber uint64            `json:"blockNumber"`
	Evidence    *SignedEvidence   `json:"evidence"`
	ActiveSync  *ActiveSyncResult `json:"activeSync,omitempty"`
}

// ActiveSyncResult is the certified value carried by an active synchronization
// transaction. The tx manager independently recomputes it from the signed read
// evidence before passing it to the local store.
type ActiveSyncResult struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	ValueHash []byte `json:"valueHash"`
	Algorithm string `json:"algorithm"`
}

type EvidenceVerifier func(*SignedEvidence) error

type DB struct {
	channelID string
	db        *leveldbhelper.DB
	verifier  EvidenceVerifier
	mutex     sync.Mutex
}

func NewDB(path, channelID string, verifier EvidenceVerifier) *DB {
	db := leveldbhelper.CreateDB(&leveldbhelper.Conf{DBPath: path})
	db.Open()
	return &DB{channelID: channelID, db: db, verifier: verifier}
}

func (d *DB) Close() {
	d.db.Close()
}

func (d *DB) Get(namespace, key string) ([]byte, error) {
	recordBytes, err := d.db.Get(committedKey(namespace, key))
	if err != nil || recordBytes == nil {
		return nil, err
	}
	record := &ValueRecord{}
	if err := json.Unmarshal(recordBytes, record); err != nil {
		return nil, fmt.Errorf("decode relaxed state %s/%s: %w", namespace, key, err)
	}
	return append([]byte(nil), record.Value...), nil
}

func (d *DB) GetRecord(namespace, key string) (*ValueRecord, error) {
	recordBytes, err := d.db.Get(committedKey(namespace, key))
	if err != nil || recordBytes == nil {
		return nil, err
	}
	record := &ValueRecord{}
	if err := json.Unmarshal(recordBytes, record); err != nil {
		return nil, fmt.Errorf("decode relaxed state record %s/%s: %w", namespace, key, err)
	}
	return record, nil
}

func (d *DB) Stage(simulation *Simulation) error {
	if simulation == nil {
		return nil
	}
	purpose := simulationPurpose(simulation)
	if len(simulation.Writes) == 0 && purpose != ActiveSyncPurpose {
		return nil
	}
	if purpose == ActiveSyncPurpose && (len(simulation.Reads) != 1 || len(simulation.Writes) != 0) {
		return fmt.Errorf("active sync requires exactly one relaxed read and no relaxed writes")
	}
	if simulation.ChannelID != d.channelID {
		return fmt.Errorf("relaxed simulation channel %q does not match %q", simulation.ChannelID, d.channelID)
	}
	if simulation.TxID == "" {
		return fmt.Errorf("relaxed simulation txID must not be empty")
	}
	simulation.Purpose = purpose
	normalizeReads(simulation.Reads)
	normalizeWrites(simulation.Writes)
	encoded, err := json.Marshal(simulation)
	if err != nil {
		return err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()
	existing, err := d.db.Get(pendingKey(simulation.TxID))
	if err != nil {
		return err
	}
	if existing != nil {
		if !bytes.Equal(existing, encoded) {
			return fmt.Errorf("txid %s was already staged with a different relaxed delta", simulation.TxID)
		}
		return nil
	}
	return d.db.Put(pendingKey(simulation.TxID), encoded, true)
}

func (d *DB) StoreEvidence(txID string, evidence *SignedEvidence) error {
	if evidence == nil {
		return fmt.Errorf("relaxed evidence must not be nil")
	}
	if evidence.Payload.ChannelID != d.channelID || evidence.Payload.TxID != txID {
		return fmt.Errorf("relaxed evidence does not match channel/txid")
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.db.Put(pendingEvidenceKey(txID), encoded, true)
}

func (d *DB) Pending(txID string) (*Simulation, error) {
	encoded, err := d.db.Get(pendingKey(txID))
	if err != nil || encoded == nil {
		return nil, err
	}
	simulation := &Simulation{}
	if err := json.Unmarshal(encoded, simulation); err != nil {
		return nil, err
	}
	return simulation, nil
}

func (d *DB) Validate(txID string, bundle [][]byte, activeSync *ActiveSyncResult) error {
	pending, err := d.Pending(txID)
	if err != nil {
		return err
	}
	storedEvidence, err := d.db.Get(pendingEvidenceKey(txID))
	if err != nil {
		return err
	}

	var localEvidenceFound bool
	for _, encoded := range bundle {
		evidence := &SignedEvidence{}
		if err := json.Unmarshal(encoded, evidence); err != nil {
			return fmt.Errorf("decode relaxed evidence for txid %s: %w", txID, err)
		}
		if evidence.Payload.Domain != LocalEndorsementDomain ||
			evidence.Payload.ChannelID != d.channelID ||
			evidence.Payload.TxID != txID {
			return fmt.Errorf("relaxed evidence is not bound to channel %s txid %s", d.channelID, txID)
		}
		if evidence.Payload.Purpose != StateUpdatePurpose && evidence.Payload.Purpose != ActiveSyncPurpose {
			return fmt.Errorf("unsupported relaxed evidence purpose %q", evidence.Payload.Purpose)
		}
		if err := validateReadEvidence(evidence.Payload.Reads); err != nil {
			return err
		}
		if err := validateWriteEvidence(evidence.Payload.Writes); err != nil {
			return err
		}
		if d.verifier != nil {
			if err := d.verifier(evidence); err != nil {
				return fmt.Errorf("verify relaxed evidence for txid %s: %w", txID, err)
			}
		}
		if storedEvidence != nil && bytes.Equal(storedEvidence, encoded) {
			localEvidenceFound = true
		}
	}

	if pending == nil {
		// A peer that did not endorse can still validate the signed observation
		// bundle and adopt the certified value after the transaction is VALID.
		return nil
	}
	if storedEvidence == nil {
		return fmt.Errorf("txid %s has a pending relaxed delta without local endorsement", txID)
	}
	if !localEvidenceFound {
		return fmt.Errorf("txid %s evidence bundle omits this peer's local endorsement", txID)
	}

	evidence := &SignedEvidence{}
	if err := json.Unmarshal(storedEvidence, evidence); err != nil {
		return err
	}
	if !writesMatchEvidence(pending.Writes, evidence.Payload.Writes) {
		return fmt.Errorf("txid %s pending relaxed delta does not match local endorsement", txID)
	}
	if !readsMatchEvidence(pending.Reads, evidence.Payload.Reads) ||
		simulationPurpose(pending) != evidence.Payload.Purpose {
		return fmt.Errorf("txid %s pending relaxed reads do not match local endorsement", txID)
	}
	if evidence.Payload.Purpose == ActiveSyncPurpose {
		if activeSync == nil {
			return fmt.Errorf("txid %s active sync evidence has no certified result", txID)
		}
		if !activeSyncMatchesRead(activeSync, pending.Reads[0]) {
			return fmt.Errorf("txid %s active sync result does not match the endorsed key or value hash", txID)
		}
	} else if activeSync != nil {
		return fmt.Errorf("txid %s carries an active sync result for a state-update transaction", txID)
	}
	return nil
}

func (d *DB) Commit(txID string, blockNumber uint64, activeSync *ActiveSyncResult) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	pendingBytes, err := d.db.Get(pendingKey(txID))
	if err != nil {
		return err
	}
	if pendingBytes == nil {
		if activeSync == nil {
			return nil
		}
		if !validActiveSyncResult(activeSync) {
			return fmt.Errorf("txid %s active sync commit has an invalid certified result", txID)
		}
		record := &ValueRecord{
			Namespace:   activeSync.Namespace,
			Key:         activeSync.Key,
			Value:       append([]byte(nil), activeSync.Value...),
			TxID:        txID,
			BlockNumber: blockNumber,
			ActiveSync:  cloneActiveSyncResult(activeSync),
		}
		recordBytes, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return d.db.Put(committedKey(activeSync.Namespace, activeSync.Key), recordBytes, true)
	}
	evidenceBytes, err := d.db.Get(pendingEvidenceKey(txID))
	if err != nil || evidenceBytes == nil {
		return err
	}
	pending := &Simulation{}
	if err := json.Unmarshal(pendingBytes, pending); err != nil {
		return err
	}
	evidence := &SignedEvidence{}
	if err := json.Unmarshal(evidenceBytes, evidence); err != nil {
		return err
	}

	batch := &leveldb.Batch{}
	if simulationPurpose(pending) == ActiveSyncPurpose {
		if activeSync == nil {
			return fmt.Errorf("txid %s active sync commit is missing its certified result", txID)
		}
		record := &ValueRecord{
			Namespace:   activeSync.Namespace,
			Key:         activeSync.Key,
			Value:       append([]byte(nil), activeSync.Value...),
			TxID:        txID,
			BlockNumber: blockNumber,
			Evidence:    evidence,
			ActiveSync:  cloneActiveSyncResult(activeSync),
		}
		recordBytes, err := json.Marshal(record)
		if err != nil {
			return err
		}
		batch.Put(committedKey(activeSync.Namespace, activeSync.Key), recordBytes)
	}
	for _, write := range pending.Writes {
		key := committedKey(write.Namespace, write.Key)
		if write.Delete {
			batch.Delete(key)
			continue
		}
		record := &ValueRecord{
			Namespace:   write.Namespace,
			Key:         write.Key,
			Value:       append([]byte(nil), write.Value...),
			TxID:        txID,
			BlockNumber: blockNumber,
			Evidence:    evidence,
		}
		recordBytes, err := json.Marshal(record)
		if err != nil {
			return err
		}
		batch.Put(key, recordBytes)
	}
	batch.Put(committedEvidenceKey(txID), evidenceBytes)
	batch.Delete(pendingKey(txID))
	batch.Delete(pendingEvidenceKey(txID))
	return d.db.WriteBatch(batch, true)
}

func (d *DB) Discard(txID string) error {
	batch := &leveldb.Batch{}
	batch.Delete(pendingKey(txID))
	batch.Delete(pendingEvidenceKey(txID))
	return d.db.WriteBatch(batch, true)
}

func NewEvidencePayload(simulation *Simulation, proposalHash, canonicalResultHash []byte) EvidencePayload {
	reads := make([]ReadEvidence, 0, len(simulation.Reads))
	for _, read := range simulation.Reads {
		hash := sha256.Sum256(read.Value)
		reads = append(reads, ReadEvidence{
			Namespace: read.Namespace,
			Key:       read.Key,
			Value:     append([]byte(nil), read.Value...),
			ValueHash: hash[:],
		})
	}
	writes := make([]WriteEvidence, 0, len(simulation.Writes))
	for _, write := range simulation.Writes {
		hash := sha256.Sum256(write.Value)
		writes = append(writes, WriteEvidence{
			Namespace: write.Namespace,
			Key:       write.Key,
			Value:     append([]byte(nil), write.Value...),
			ValueHash: hash[:],
			Delete:    write.Delete,
		})
	}
	return EvidencePayload{
		Domain:              LocalEndorsementDomain,
		ChannelID:           simulation.ChannelID,
		TxID:                simulation.TxID,
		Purpose:             simulationPurpose(simulation),
		ProposalHash:        append([]byte(nil), proposalHash...),
		CanonicalResultHash: append([]byte(nil), canonicalResultHash...),
		Reads:               reads,
		Writes:              writes,
	}
}

func (e *SignedEvidence) PayloadBytes() ([]byte, error) {
	return json.Marshal(e.Payload)
}

func (e *SignedEvidence) SigningBytes() ([]byte, error) {
	payload, err := e.PayloadBytes()
	if err != nil {
		return nil, err
	}
	return append(payload, e.Endorser...), nil
}

func EncodeSignedEvidence(evidence *SignedEvidence) ([]byte, error) {
	return json.Marshal(evidence)
}

func EvidenceMessage(evidence *SignedEvidence) (string, error) {
	encoded, err := EncodeSignedEvidence(evidence)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(encoded), nil
}

func normalizeWrites(writes []Write) {
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Namespace == writes[j].Namespace {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Namespace < writes[j].Namespace
	})
}

func normalizeReads(reads []Read) {
	sort.Slice(reads, func(i, j int) bool {
		if reads[i].Namespace == reads[j].Namespace {
			return reads[i].Key < reads[j].Key
		}
		return reads[i].Namespace < reads[j].Namespace
	})
}

func simulationPurpose(simulation *Simulation) string {
	if simulation == nil {
		return ""
	}
	if simulation.Purpose != "" {
		return simulation.Purpose
	}
	if len(simulation.Writes) > 0 {
		return StateUpdatePurpose
	}
	return ""
}

func validateReadEvidence(reads []ReadEvidence) error {
	for _, read := range reads {
		hash := sha256.Sum256(read.Value)
		if !bytes.Equal(hash[:], read.ValueHash) {
			return fmt.Errorf("relaxed read evidence value hash mismatch for %s/%s", read.Namespace, read.Key)
		}
	}
	return nil
}

func validateWriteEvidence(writes []WriteEvidence) error {
	for _, write := range writes {
		hash := sha256.Sum256(write.Value)
		if !bytes.Equal(hash[:], write.ValueHash) {
			return fmt.Errorf("relaxed evidence value hash mismatch for %s/%s", write.Namespace, write.Key)
		}
	}
	return nil
}

func writesMatchEvidence(writes []Write, evidence []WriteEvidence) bool {
	if len(writes) != len(evidence) {
		return false
	}
	for index, write := range writes {
		item := evidence[index]
		if write.Namespace != item.Namespace || write.Key != item.Key ||
			write.Delete != item.Delete || !bytes.Equal(write.Value, item.Value) {
			return false
		}
	}
	return true
}

func readsMatchEvidence(reads []Read, evidence []ReadEvidence) bool {
	if len(reads) != len(evidence) {
		return false
	}
	for index, read := range reads {
		item := evidence[index]
		if read.Namespace != item.Namespace || read.Key != item.Key ||
			!bytes.Equal(read.Value, item.Value) {
			return false
		}
	}
	return true
}

func activeSyncMatchesRead(result *ActiveSyncResult, read Read) bool {
	if !validActiveSyncResult(result) || result.Namespace != read.Namespace || result.Key != read.Key {
		return false
	}
	return true
}

func validActiveSyncResult(result *ActiveSyncResult) bool {
	if result == nil || result.Algorithm != MedianJSONPriceAlgorithm || result.Namespace == "" || result.Key == "" {
		return false
	}
	hash := sha256.Sum256(result.Value)
	return bytes.Equal(hash[:], result.ValueHash)
}

func cloneActiveSyncResult(result *ActiveSyncResult) *ActiveSyncResult {
	if result == nil {
		return nil
	}
	return &ActiveSyncResult{
		Namespace: result.Namespace,
		Key:       result.Key,
		Value:     append([]byte(nil), result.Value...),
		ValueHash: append([]byte(nil), result.ValueHash...),
		Algorithm: result.Algorithm,
	}
}

func committedKey(namespace, key string) []byte {
	return []byte("c|" + encodeKey(namespace) + "|" + encodeKey(key))
}

func pendingKey(txID string) []byte {
	return []byte("p|" + encodeKey(txID))
}

func pendingEvidenceKey(txID string) []byte {
	return []byte("s|" + encodeKey(txID))
}

func committedEvidenceKey(txID string) []byte {
	return []byte("e|" + encodeKey(txID))
}

func encodeKey(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
