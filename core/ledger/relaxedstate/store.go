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

const LocalEndorsementDomain = "GRAND_LOCAL_STATE_ENDORSEMENT_V1"

type Write struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	Delete    bool   `json:"delete,omitempty"`
}

type Simulation struct {
	ChannelID string  `json:"channelId"`
	TxID      string  `json:"txId"`
	Writes    []Write `json:"writes"`
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
	ProposalHash        []byte          `json:"proposalHash"`
	CanonicalResultHash []byte          `json:"canonicalResultHash"`
	Writes              []WriteEvidence `json:"writes"`
}

type SignedEvidence struct {
	Payload   EvidencePayload `json:"payload"`
	Endorser  []byte          `json:"endorser"`
	Signature []byte          `json:"signature"`
}

type ValueRecord struct {
	Namespace   string          `json:"namespace"`
	Key         string          `json:"key"`
	Value       []byte          `json:"value"`
	TxID        string          `json:"txId"`
	BlockNumber uint64          `json:"blockNumber"`
	Evidence    *SignedEvidence `json:"evidence"`
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
	if simulation == nil || len(simulation.Writes) == 0 {
		return nil
	}
	if simulation.ChannelID != d.channelID {
		return fmt.Errorf("relaxed simulation channel %q does not match %q", simulation.ChannelID, d.channelID)
	}
	if simulation.TxID == "" {
		return fmt.Errorf("relaxed simulation txID must not be empty")
	}
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

func (d *DB) Validate(txID string, bundle [][]byte) error {
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
	return nil
}

func (d *DB) Commit(txID string, blockNumber uint64) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	pendingBytes, err := d.db.Get(pendingKey(txID))
	if err != nil || pendingBytes == nil {
		return err
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
		ProposalHash:        append([]byte(nil), proposalHash...),
		CanonicalResultHash: append([]byte(nil), canonicalResultHash...),
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
