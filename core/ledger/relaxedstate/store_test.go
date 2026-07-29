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
