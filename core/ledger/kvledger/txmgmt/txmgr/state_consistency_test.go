/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txmgr

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/core/ledger/cceventmgmt"
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

func TestDeployedIdentityProgramKeepsNormalStateCanonical(t *testing.T) {
	env := testEnvsMap[levelDBtestEnvName]
	env.init(t, "deployed-identity-consistency-program", nil)
	defer env.cleanup()

	deployContractProgram(t, env.getVDB(), "identitycc", stateconsistency.IdentityChange)
	txMgr := env.getTxMgr()
	legacyPolicy, err := stateconsistency.NewPolicy(&stateconsistency.Manifest{
		Version: 1,
		Rules: []stateconsistency.Rule{{
			Namespace: "identitycc",
			Level:     "strong",
		}},
	})
	require.NoError(t, err)
	txMgr.stateConsistency = legacyPolicy
	help := newTxMgrTestHelper(t, txMgr)
	simulator, err := txMgr.NewTxSimulator("identity-write")
	require.NoError(t, err)
	require.NoError(t, simulator.SetState("identitycc", "asset", []byte("value")))
	validationKey := peer.MetaDataKeys_VALIDATION_PARAMETER.String()
	require.NoError(t, simulator.SetStateMetadata(
		"identitycc",
		"asset",
		map[string][]byte{validationKey: []byte("identity-policy")},
	))
	results, err := simulator.GetTxSimulationResults()
	require.NoError(t, err)
	require.Empty(t, simulator.(*txSimulator).GrandRelaxedStateSimulation().Writes)
	simulator.Done()
	help.validateAndCommitRWSet(results.PubSimulationResults)

	query, err := txMgr.NewQueryExecutor("identity-query")
	require.NoError(t, err)
	defer query.Done()
	value, err := query.GetState("identitycc", "asset")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	metadata, err := query.GetStateMetadata("identitycc", "asset")
	require.NoError(t, err)
	require.Equal(t, []byte("identity-policy"), metadata[validationKey])
	require.Equal(t, []byte("0"), metadata[stateconsistency.NumericMetadataKey])
	require.Equal(t, []byte(stateconsistency.Normal), metadata[stateconsistency.MetadataKey])

	simulator, err = txMgr.NewTxSimulator("identity-reserved-metadata")
	require.NoError(t, err)
	defer simulator.Done()
	err = simulator.SetStateMetadata(
		"identitycc",
		"asset",
		map[string][]byte{stateconsistency.NumericMetadataKey: []byte("8")},
	)
	require.ErrorContains(t, err, "managed by the deployed GraND consistency program")
}

func TestDeployedIncrementProgramAdvancesRelaxedTierWithoutThresholdTrigger(t *testing.T) {
	env := testEnvsMap[levelDBtestEnvName]
	env.init(t, "deployed-increment-consistency-program", nil)
	defer env.cleanup()

	deployContractProgram(t, env.getVDB(), "incrementcc", stateconsistency.IncrementChange)
	txMgr := env.getTxMgr()

	first, err := txMgr.NewTxSimulator("increment-first-write")
	require.NoError(t, err)
	require.NoError(t, first.SetState("incrementcc", "asset", []byte("one")))
	err = first.SetStateMetadata(
		"incrementcc",
		"asset",
		map[string][]byte{peer.MetaDataKeys_VALIDATION_PARAMETER.String(): []byte("policy")},
	)
	require.ErrorContains(t, err, "not supported for peer-local relaxed state")
	_, err = first.GetTxSimulationResults()
	require.NoError(t, err)
	firstSimulation := first.(*txSimulator).GrandRelaxedStateSimulation()
	require.Len(t, firstSimulation.Writes, 1)
	require.Equal(t, uint64(1), firstSimulation.Writes[0].Tier)
	require.Equal(t, uint64(10), firstSimulation.Writes[0].TierThreshold)
	require.False(t, firstSimulation.Writes[0].PreventiveSync)
	first.Done()

	seed := &relaxedstate.Simulation{
		ChannelID: txMgr.ledgerid,
		TxID:      "seed-deployed-tier",
		Writes: []relaxedstate.Write{{
			Namespace:     "incrementcc",
			Key:           "existing",
			Value:         []byte("four"),
			Tier:          4,
			TierThreshold: 10,
		}},
	}
	require.NoError(t, txMgr.relaxedState.Stage(seed))
	require.NoError(t, txMgr.relaxedState.StoreEvidence(seed.TxID, &relaxedstate.SignedEvidence{
		Payload:   relaxedstate.NewEvidencePayload(seed, nil, nil),
		Endorser:  []byte("test-peer"),
		Signature: []byte("test-signature"),
	}))
	require.NoError(t, txMgr.relaxedState.Commit(seed.TxID, 1, nil))

	next, err := txMgr.NewTxSimulator("increment-next-write")
	require.NoError(t, err)
	value, err := next.GetState("incrementcc", "existing")
	require.NoError(t, err)
	require.Equal(t, []byte("four"), value)
	require.NoError(t, next.SetState("incrementcc", "existing", []byte("five")))
	_, err = next.GetTxSimulationResults()
	require.NoError(t, err)
	nextSimulation := next.(*txSimulator).GrandRelaxedStateSimulation()
	require.Len(t, nextSimulation.Reads, 1)
	require.Equal(t, uint64(4), nextSimulation.Reads[0].Tier)
	require.Len(t, nextSimulation.Writes, 1)
	require.Equal(t, uint64(5), nextSimulation.Writes[0].Tier)
	require.Equal(t, uint64(10), nextSimulation.Writes[0].TierThreshold)
	require.False(t, nextSimulation.Writes[0].PreventiveSync)
	next.Done()
}

func deployContractProgram(
	t *testing.T,
	db interface {
		HandleChaincodeDeploy(*cceventmgmt.ChaincodeDefinition, []byte) error
		ChaincodeDeployDone(bool)
	},
	chaincodeName,
	changeFunction string,
) {
	t.Helper()
	program := stateconsistency.ContractProgram{
		SchemaVersion:        stateconsistency.ContractProgramSchemaVersion,
		LevelEncoding:        stateconsistency.SignedIntegerLevelEncoding,
		InitialLevel:         stateconsistency.NormalNumericLevel,
		ChangeFunction:       changeFunction,
		RelaxedTierThreshold: 10,
	}
	artifact, err := json.Marshal(program)
	require.NoError(t, err)
	metadataTar := bytes.NewBuffer(nil)
	writer := tar.NewWriter(metadataTar)
	require.NoError(t, writer.WriteHeader(&tar.Header{
		Name: stateconsistency.ContractProgramArtifact,
		Mode: 0o644,
		Size: int64(len(artifact)),
	}))
	_, err = writer.Write(artifact)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, db.HandleChaincodeDeploy(&cceventmgmt.ChaincodeDefinition{
		Name:    chaincodeName,
		Version: "1.0",
		Hash:    []byte(chaincodeName + ":package-id"),
	}, metadataTar.Bytes()))
	db.ChaincodeDeployDone(true)
}
