/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txmgr

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	stateconsistency "github.com/hyperledger/fabric/core/ledger/consistency"
	"github.com/hyperledger/fabric/core/ledger/relaxedstate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTxSimulatorAssignsStateConsistencyLevels(t *testing.T) {
	env := testEnvsMap[levelDBtestEnvName]
	env.init(t, "state-consistency-levels", nil)
	defer env.cleanup()

	policy, err := stateconsistency.NewPolicy(&stateconsistency.Manifest{
		Version:      1,
		DefaultLevel: "normal",
		Rules: []stateconsistency.Rule{
			{Namespace: "basic", Level: "strong"},
			{Namespace: "basic", KeyPrefix: "normal:", Level: "normal"},
			{Namespace: "basic", Key: "relaxed:key", Level: "relaxed"},
		},
	})
	require.NoError(t, err)

	txMgr := env.getTxMgr()
	txMgr.stateConsistency = policy
	help := newTxMgrTestHelper(t, txMgr)
	validationKey := peer.MetaDataKeys_VALIDATION_PARAMETER.String()

	simulator, err := txMgr.NewTxSimulator("set-classified-state")
	require.NoError(t, err)

	// A relaxed write is captured in the peer-local simulation and excluded
	// from Fabric's canonical RWSet.
	require.NoError(t, simulator.SetState("basic", "relaxed:key", []byte("relaxed-value")))

	require.NoError(t, simulator.SetState("basic", "normal:key", []byte("normal-value")))
	require.NoError(t, simulator.SetStateMetadata(
		"basic",
		"normal:key",
		map[string][]byte{validationKey: []byte("normal-policy")},
	))

	require.NoError(t, simulator.SetState("basic", "strong:key", []byte("strong-value")))
	require.NoError(t, simulator.SetState("unconfigured", "key", []byte("implicit-normal-value")))
	results, err := simulator.GetTxSimulationResults()
	require.NoError(t, err)
	localSimulation := simulator.(*txSimulator).GrandRelaxedStateSimulation()
	require.Equal(t, "set-classified-state", localSimulation.TxID)
	require.Equal(t, []byte("relaxed-value"), localSimulation.Writes[0].Value)
	canonicalBytes, err := proto.Marshal(results.PubSimulationResults)
	require.NoError(t, err)
	require.NotContains(t, string(canonicalBytes), "relaxed:key")
	simulator.Done()
	help.validateAndCommitRWSet(results.PubSimulationResults)

	query, err := txMgr.NewQueryExecutor("read-classified-state")
	require.NoError(t, err)
	defer query.Done()

	assertLevel := func(namespace, key string, expected stateconsistency.Level) {
		t.Helper()
		level, err := stateconsistency.GetStateLevel(query, namespace, key)
		require.NoError(t, err)
		require.Equal(t, expected, level)
	}
	assertLevel("basic", "strong:key", stateconsistency.Strong)
	assertLevel("basic", "normal:key", stateconsistency.Normal)
	assertLevel("basic", "relaxed:key", stateconsistency.Relaxed)
	assertLevel("unconfigured", "key", stateconsistency.Normal)

	dbLevel, err := env.getVDB().GetStateConsistencyLevel("basic", "normal:key")
	require.NoError(t, err)
	require.Equal(t, stateconsistency.Normal, dbLevel)

	strongMetadata, err := query.GetStateMetadata("basic", "strong:key")
	require.NoError(t, err)
	require.Equal(t, []byte("strong"), strongMetadata[stateconsistency.MetadataKey])

	relaxedMetadata, err := query.GetStateMetadata("basic", "relaxed:key")
	require.NoError(t, err)
	require.Equal(t, []byte("relaxed"), relaxedMetadata[stateconsistency.MetadataKey])

	normalMetadata, err := query.GetStateMetadata("basic", "normal:key")
	require.NoError(t, err)
	require.Equal(t, []byte("normal-policy"), normalMetadata[validationKey])
	require.Equal(t, []byte("normal"), normalMetadata[stateconsistency.MetadataKey])

	implicitMetadata, err := query.GetStateMetadata("unconfigured", "key")
	require.NoError(t, err)
	require.NotContains(t, implicitMetadata, stateconsistency.MetadataKey)
}

func TestTxSimulatorPropagatesTierAndMarksFixedThreshold(t *testing.T) {
	env := testEnvsMap[levelDBtestEnvName]
	env.init(t, "relaxed-tier-threshold", nil)
	defer env.cleanup()

	threshold := uint64(4)
	policy, err := stateconsistency.NewPolicy(&stateconsistency.Manifest{
		Version: 1,
		Rules: []stateconsistency.Rule{
			{Namespace: "source", KeyPrefix: "quote:", Level: "relaxed"},
			{Namespace: "derived", KeyPrefix: "quote:", Level: "relaxed", TierThreshold: &threshold},
		},
	})
	require.NoError(t, err)
	txMgr := env.getTxMgr()
	txMgr.stateConsistency = policy

	seed := &relaxedstate.Simulation{
		ChannelID: txMgr.ledgerid,
		TxID:      "seed-tier",
		Writes: []relaxedstate.Write{{
			Namespace: "source",
			Key:       "quote:BTC-USD",
			Value:     []byte("103"),
			Tier:      3,
		}},
	}
	require.NoError(t, txMgr.relaxedState.Stage(seed))
	require.NoError(t, txMgr.relaxedState.StoreEvidence(seed.TxID, &relaxedstate.SignedEvidence{
		Payload:   relaxedstate.NewEvidencePayload(seed, nil, nil),
		Endorser:  []byte("test-peer"),
		Signature: []byte("test-signature"),
	}))
	require.NoError(t, txMgr.relaxedState.Commit(seed.TxID, 1, nil))

	simulator, err := txMgr.NewTxSimulator("propagate-tier")
	require.NoError(t, err)
	value, err := simulator.GetState("source", "quote:BTC-USD")
	require.NoError(t, err)
	require.Equal(t, []byte("103"), value)
	require.NoError(t, simulator.SetState("derived", "quote:BTC-USD", []byte("104")))
	_, err = simulator.GetTxSimulationResults()
	require.NoError(t, err)
	defer simulator.Done()

	localSimulation := simulator.(*txSimulator).GrandRelaxedStateSimulation()
	require.Equal(t, uint64(3), localSimulation.Reads[0].Tier)
	require.Equal(t, uint64(4), localSimulation.Writes[0].Tier)
	require.Equal(t, uint64(4), localSimulation.Writes[0].TierThreshold)
	require.True(t, localSimulation.Writes[0].PreventiveSync)
}

func TestTxSimulatorRejectsInvalidStateConsistencyLevel(t *testing.T) {
	env := testEnvsMap[levelDBtestEnvName]
	env.init(t, "invalid-state-consistency-level", nil)
	defer env.cleanup()

	simulator, err := env.getTxMgr().NewTxSimulator("invalid-classification")
	require.NoError(t, err)
	defer simulator.Done()

	err = simulator.SetStateMetadata(
		"basic",
		"key",
		map[string][]byte{stateconsistency.MetadataKey: []byte("eventual")},
	)
	require.ErrorContains(t, err, "invalid state consistency level")
}
