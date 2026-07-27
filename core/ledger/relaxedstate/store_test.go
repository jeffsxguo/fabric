/*
Copyright 2026 GraND Authors.

SPDX-License-Identifier: Apache-2.0
*/

package relaxedstate

import (
	"crypto/ed25519"
	"crypto/rand"
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
	require.NoError(t, db.Validate("tx1", [][]byte{encodedEvidence}))
	require.NoError(t, db.Commit("tx1", 7))

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

	err := db.Validate("tx1", nil)
	require.ErrorContains(t, err, "omits this peer's local endorsement")

	evidence.Payload.Writes[0].Value = []byte("999")
	changed, err := json.Marshal(evidence)
	require.NoError(t, err)
	err = db.Validate("tx1", [][]byte{changed})
	require.ErrorContains(t, err, "value hash mismatch")
}

type errInvalidTestSignature struct{}

func (errInvalidTestSignature) Error() string { return "invalid test signature" }
