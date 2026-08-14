/*
Copyright 2026 GraND Authors.

SPDX-License-Identifier: Apache-2.0
*/

package relaxedstate

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStageValidateAndCommitPeerLocalValue(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	db := NewDB(t.TempDir(), "mychannel", func(evidence *SignedEvidence) error {
		signingBytes, err := evidence.SigningBytes()
		if err != nil {
			return err
		}
		if !ed25519.Verify(publicKey, signingBytes, evidence.Signature) {
			return errInvalidTestSignature{}
		}
		return nil
	})
	defer db.Close()

	simulation := &Simulation{
		ChannelID: "mychannel",
		TxID:      "tx1",
		Writes: []Write{
			{Namespace: "grandmarket", Key: "quote:BTC-USD", Value: []byte("101")},
		},
	}
	require.NoError(t, db.Stage(simulation))

	evidence := &SignedEvidence{
		Payload:  NewEvidencePayload(simulation, []byte("proposal"), []byte("canonical")),
		Endorser: []byte("peer0.org1.example.com"),
	}
	signingBytes, err := evidence.SigningBytes()
	require.NoError(t, err)
	evidence.Signature = ed25519.Sign(privateKey, signingBytes)
	require.NoError(t, db.StoreEvidence("tx1", evidence))

	encodedEvidence, err := json.Marshal(evidence)
	require.NoError(t, err)
	require.NoError(t, db.Validate("tx1", [][]byte{encodedEvidence}, nil))
	require.NoError(t, db.Commit("tx1", 7, nil))

	value, err := db.Get("grandmarket", "quote:BTC-USD")
	require.NoError(t, err)
	require.Equal(t, []byte("101"), value)
	record, err := db.GetRecord("grandmarket", "quote:BTC-USD")
	require.NoError(t, err)
	require.Equal(t, uint64(7), record.BlockNumber)
	require.Equal(t, "tx1", record.TxID)
	require.Equal(t, evidence.Signature, record.Evidence.Signature)
}

func TestRejectsChangedOrOmittedLocalEvidence(t *testing.T) {
	db := NewDB(t.TempDir(), "mychannel", nil)
	defer db.Close()

	simulation := &Simulation{
		ChannelID: "mychannel",
		TxID:      "tx1",
		Writes: []Write{
			{Namespace: "grandmarket", Key: "quote:BTC-USD", Value: []byte("101")},
		},
	}
	require.NoError(t, db.Stage(simulation))
	evidence := &SignedEvidence{
		Payload:   NewEvidencePayload(simulation, nil, nil),
		Endorser:  []byte("peer"),
		Signature: []byte("signature"),
	}
	require.NoError(t, db.StoreEvidence("tx1", evidence))

	err := db.Validate("tx1", nil, nil)
	require.ErrorContains(t, err, "omits this peer's local endorsement")

	evidence.Payload.Writes[0].Value = []byte("999")
	changed, err := json.Marshal(evidence)
	require.NoError(t, err)
	err = db.Validate("tx1", [][]byte{changed}, nil)
	require.ErrorContains(t, err, "value hash mismatch")
}

func TestActiveSyncCommitsCertifiedValue(t *testing.T) {
	db := NewDB(t.TempDir(), "mychannel", nil)
	defer db.Close()

	localValue := []byte(`{"symbol":"BTC-USD","sourceId":"source-a","price":101}`)
	simulation := &Simulation{
		ChannelID: "mychannel",
		TxID:      "sync1",
		Purpose:   ActiveSyncPurpose,
		Reads: []Read{
			{Namespace: "grandmarket", Key: "quote:BTC-USD", Value: localValue},
		},
	}
	require.NoError(t, db.Stage(simulation))
	evidence := &SignedEvidence{
		Payload:   NewEvidencePayload(simulation, nil, nil),
		Endorser:  []byte("peer"),
		Signature: []byte("signature"),
	}
	require.NoError(t, db.StoreEvidence("sync1", evidence))
	encoded, err := json.Marshal(evidence)
	require.NoError(t, err)

	median := []byte(`{"symbol":"BTC-USD","sourceId":"source-b","price":103}`)
	hash := sha256.Sum256(median)
	result := &ActiveSyncResult{
		Namespace: "grandmarket",
		Key:       "quote:BTC-USD",
		Value:     median,
		ValueHash: hash[:],
		Algorithm: MedianJSONPriceAlgorithm,
	}
	require.NoError(t, db.Validate("sync1", [][]byte{encoded}, result))
	require.NoError(t, db.Commit("sync1", 8, result))

	value, err := db.Get("grandmarket", "quote:BTC-USD")
	require.NoError(t, err)
	require.Equal(t, median, value)
	record, err := db.GetRecord("grandmarket", "quote:BTC-USD")
	require.NoError(t, err)
	require.Equal(t, result, record.ActiveSync)
	require.Zero(t, record.Tier)
}

func TestTierThresholdPersistsRequestAndActiveSyncClearsIt(t *testing.T) {
	db := NewDB(t.TempDir(), "mychannel", nil)
	defer db.Close()

	writeSimulation := &Simulation{
		ChannelID: "mychannel",
		TxID:      "propagate1",
		Reads: []Read{{
			Namespace: "source",
			Key:       "quote:BTC-USD",
			Value:     []byte(`{"price":103}`),
			Tier:      1,
		}},
		Writes: []Write{{
			Namespace:      "increment",
			Key:            "quote:BTC-USD",
			Value:          []byte(`{"price":104}`),
			Tier:           2,
			TierThreshold:  2,
			PreventiveSync: true,
		}},
	}
	require.NoError(t, db.Stage(writeSimulation))
	writeEvidence := &SignedEvidence{
		Payload:   NewEvidencePayload(writeSimulation, []byte("proposal"), []byte("canonical")),
		Endorser:  []byte("peer"),
		Signature: []byte("signature"),
	}
	require.NoError(t, db.StoreEvidence(writeSimulation.TxID, writeEvidence))
	encodedWriteEvidence, err := json.Marshal(writeEvidence)
	require.NoError(t, err)
	require.NoError(t, db.Validate(writeSimulation.TxID, [][]byte{encodedWriteEvidence}, nil))
	requests, err := db.CommitAndCollectPreventiveSyncRequests(writeSimulation.TxID, 9, nil)
	require.NoError(t, err)
	require.Equal(t, []PreventiveSyncRequest{{
		ChannelID:   "mychannel",
		Namespace:   "increment",
		Key:         "quote:BTC-USD",
		Tier:        2,
		Threshold:   2,
		TxID:        "propagate1",
		BlockNumber: 9,
	}}, requests)

	record, err := db.GetRecord("increment", "quote:BTC-USD")
	require.NoError(t, err)
	require.Equal(t, uint64(2), record.Tier)
	require.Equal(t, uint64(2), record.TierThreshold)
	request, err := db.GetPreventiveSyncRequest("increment", "quote:BTC-USD")
	require.NoError(t, err)
	require.Equal(t, requests[0], *request)

	syncSimulation := &Simulation{
		ChannelID: "mychannel",
		TxID:      "sync1",
		Purpose:   ActiveSyncPurpose,
		Reads: []Read{{
			Namespace: "increment",
			Key:       "quote:BTC-USD",
			Value:     []byte(`{"price":104}`),
			Tier:      2,
		}},
	}
	require.NoError(t, db.Stage(syncSimulation))
	syncEvidence := &SignedEvidence{
		Payload:   NewEvidencePayload(syncSimulation, []byte("sync-proposal"), []byte("sync-canonical")),
		Endorser:  []byte("peer"),
		Signature: []byte("signature"),
	}
	require.NoError(t, db.StoreEvidence(syncSimulation.TxID, syncEvidence))
	encodedSyncEvidence, err := json.Marshal(syncEvidence)
	require.NoError(t, err)
	hash := sha256.Sum256(syncSimulation.Reads[0].Value)
	result := &ActiveSyncResult{
		Namespace: syncSimulation.Reads[0].Namespace,
		Key:       syncSimulation.Reads[0].Key,
		Value:     syncSimulation.Reads[0].Value,
		ValueHash: hash[:],
		Algorithm: MedianJSONPriceAlgorithm,
	}
	require.NoError(t, db.Validate(syncSimulation.TxID, [][]byte{encodedSyncEvidence}, result))
	requests, err = db.CommitAndCollectPreventiveSyncRequests(syncSimulation.TxID, 10, result)
	require.NoError(t, err)
	require.Empty(t, requests)

	record, err = db.GetRecord("increment", "quote:BTC-USD")
	require.NoError(t, err)
	require.Zero(t, record.Tier)
	require.Equal(t, uint64(2), record.TierThreshold)
	request, err = db.GetPreventiveSyncRequest("increment", "quote:BTC-USD")
	require.NoError(t, err)
	require.Nil(t, request)
}

func TestPropagatedTier(t *testing.T) {
	require.Zero(t, PropagatedTier(nil))
	require.Equal(t, uint64(8), PropagatedTier([]Read{{Tier: 2}, {Tier: 7}, {Tier: 3}}))
	require.Equal(t, ^uint64(0), PropagatedTier([]Read{{Tier: ^uint64(0)}}))
}

func TestDiscardedThresholdWriteDoesNotCreatePreventiveRequest(t *testing.T) {
	db := NewDB(t.TempDir(), "mychannel", nil)
	defer db.Close()
	simulation := &Simulation{
		ChannelID: "mychannel",
		TxID:      "invalid-propagation",
		Writes: []Write{{
			Namespace:      "increment",
			Key:            "quote:BTC-USD",
			Value:          []byte("104"),
			Tier:           1,
			TierThreshold:  1,
			PreventiveSync: true,
		}},
	}
	require.NoError(t, db.Stage(simulation))
	require.NoError(t, db.Discard(simulation.TxID))

	request, err := db.GetPreventiveSyncRequest("increment", "quote:BTC-USD")
	require.NoError(t, err)
	require.Nil(t, request)
	record, err := db.GetRecord("increment", "quote:BTC-USD")
	require.NoError(t, err)
	require.Nil(t, record)
}

func TestReadOnlyEvidenceUsesPassiveObservationPurpose(t *testing.T) {
	simulation := &Simulation{
		ChannelID: "mychannel",
		TxID:      "decision1",
		Reads: []Read{{
			Namespace: "oracle",
			Key:       "price:BTC-USD",
			Value:     []byte("104"),
		}},
	}
	payload := NewEvidencePayload(simulation, []byte("proposal"), []byte("result"))
	require.Equal(t, PassiveObservationPurpose, payload.Purpose)
	require.Len(t, payload.Reads, 1)
	require.Empty(t, payload.Writes)
}

type errInvalidTestSignature struct{}

func (errInvalidTestSignature) Error() string { return "invalid test signature" }
