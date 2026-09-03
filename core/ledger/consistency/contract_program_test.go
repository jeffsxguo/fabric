/*
Copyright 2026 GraND Authors.

SPDX-License-Identifier: Apache-2.0
*/

package consistency

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"math"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/stretchr/testify/require"
)

func TestContractProgramApply(t *testing.T) {
	identity := testContractProgram(IdentityChange)
	level, err := identity.Apply(nil)
	require.NoError(t, err)
	require.Equal(t, NormalNumericLevel, level)
	level, err = identity.Apply([]int64{StrongNumericLevel, 4, 2})
	require.NoError(t, err)
	require.Equal(t, int64(4), level)

	increment := testContractProgram(IncrementChange)
	level, err = increment.Apply(nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), level)
	level, err = increment.Apply([]int64{3, 7, 2})
	require.NoError(t, err)
	require.Equal(t, int64(8), level)
	level, err = increment.Apply([]int64{StrongNumericLevel})
	require.NoError(t, err)
	require.Equal(t, NormalNumericLevel, level)

	_, err = increment.Apply([]int64{math.MaxInt64})
	require.ErrorContains(t, err, "overflow")
	_, err = identity.Apply([]int64{-2})
	require.ErrorContains(t, err, "minimum is -1")
}

func TestParseContractProgramIsStrict(t *testing.T) {
	contents := marshalContractProgram(t, testContractProgram(IncrementChange))
	program, err := ParseContractProgram(contents)
	require.NoError(t, err)
	require.Equal(t, IncrementChange, program.ChangeFunction)
	require.Equal(t, uint64(10), program.RelaxedTierThreshold)

	_, err = ParseContractProgram([]byte(`{
		"schemaVersion":1,
		"levelEncoding":"signed-integer-v1",
		"initialLevel":0,
		"changeFunction":"increment",
		"relaxedTierThreshold":10,
		"unknown":true
	}`))
	require.ErrorContains(t, err, "unknown field")

	invalid := testContractProgram("subtract")
	_, err = ParseContractProgram(marshalContractProgram(t, invalid))
	require.ErrorContains(t, err, "unsupported consistency change function")

	invalid = testContractProgram(IdentityChange)
	invalid.InitialLevel = 1
	_, err = ParseContractProgram(marshalContractProgram(t, invalid))
	require.ErrorContains(t, err, "requires normal")
}

func TestNumericLevelMetadata(t *testing.T) {
	original := map[string][]byte{"VALIDATION_PARAMETER": []byte("policy")}
	metadata, err := WithNumericLevel(original, StrongNumericLevel)
	require.NoError(t, err)
	require.Equal(t, []byte("-1"), metadata[NumericMetadataKey])
	require.Equal(t, []byte(Strong), metadata[MetadataKey])
	require.Equal(t, []byte("policy"), metadata["VALIDATION_PARAMETER"])

	level, err := NumericLevelFromMetadata(metadata)
	require.NoError(t, err)
	require.Equal(t, StrongNumericLevel, level)

	metadata, err = WithNumericLevel(nil, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("7"), metadata[NumericMetadataKey])
	require.Equal(t, []byte(Relaxed), metadata[MetadataKey])
	level, err = NumericLevelFromMetadata(metadata)
	require.NoError(t, err)
	require.Equal(t, int64(7), level)

	level, err = NumericLevelFromMetadata(map[string][]byte{MetadataKey: []byte(Strong)})
	require.NoError(t, err)
	require.Equal(t, StrongNumericLevel, level)
}

func TestExtractContractProgram(t *testing.T) {
	artifact := marshalContractProgram(t, testContractProgram(IdentityChange))
	metadataTar := contractProgramTar(t, artifact)
	program, extracted, found, err := ExtractContractProgram(metadataTar)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, artifact, extracted)
	require.Equal(t, IdentityChange, program.ChangeFunction)

	_, _, found, err = ExtractContractProgram(emptyMetadataTar(t))
	require.NoError(t, err)
	require.False(t, found)

	_, _, found, err = ExtractContractProgram([]byte("legacy malformed DB artifact"))
	require.NoError(t, err)
	require.False(t, found)
}

func TestContractProgramRegistryStagesPersistsAndRemoves(t *testing.T) {
	provider, err := leveldbhelper.NewProvider(&leveldbhelper.Conf{
		DBPath: filepath.Join(t.TempDir(), "programs"),
	})
	require.NoError(t, err)
	defer provider.Close()
	handle := provider.GetDBHandle("channel/programs")

	registry, err := NewContractProgramRegistry(handle)
	require.NoError(t, err)
	artifact := marshalContractProgram(t, testContractProgram(IncrementChange))
	_, found := registry.Lookup("basic")
	require.False(t, found)
	staged, found, err := registry.StageDeployment(
		"basic",
		"1.0",
		"basic:package-id",
		contractProgramTar(t, artifact),
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, IncrementChange, staged.Program.ChangeFunction)
	_, found = registry.Lookup("basic")
	require.False(t, found, "staged deployment must not be visible before completion")
	require.NoError(t, registry.DeploymentDone(true))
	record, found := registry.Lookup("basic")
	require.True(t, found)
	require.Equal(t, "basic:package-id", record.PackageID)
	require.Equal(t, IncrementChange, record.Program.ChangeFunction)
	require.NotEmpty(t, record.ArtifactHash)

	reloaded, err := NewContractProgramRegistry(handle)
	require.NoError(t, err)
	record, found = reloaded.Lookup("basic")
	require.True(t, found)
	require.Equal(t, IncrementChange, record.Program.ChangeFunction)

	_, found, err = reloaded.StageDeployment(
		"basic",
		"2.0",
		"basic:new-package",
		contractProgramTar(t, marshalContractProgram(t, testContractProgram(IdentityChange))),
	)
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, reloaded.DeploymentDone(false))
	record, found = reloaded.Lookup("basic")
	require.True(t, found)
	require.Equal(t, IncrementChange, record.Program.ChangeFunction)

	_, found, err = reloaded.StageDeployment(
		"basic",
		"3.0",
		"basic:no-analysis",
		emptyMetadataTar(t),
	)
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, reloaded.DeploymentDone(true))
	_, found = reloaded.Lookup("basic")
	require.False(t, found)
}

func testContractProgram(change string) ContractProgram {
	return ContractProgram{
		SchemaVersion:        ContractProgramSchemaVersion,
		LevelEncoding:        SignedIntegerLevelEncoding,
		InitialLevel:         NormalNumericLevel,
		ChangeFunction:       change,
		RelaxedTierThreshold: 10,
	}
}

func marshalContractProgram(t *testing.T, program ContractProgram) []byte {
	t.Helper()
	contents, err := json.Marshal(program)
	require.NoError(t, err)
	return contents
}

func contractProgramTar(t *testing.T, artifact []byte) []byte {
	t.Helper()
	buffer := bytes.NewBuffer(nil)
	writer := tar.NewWriter(buffer)
	require.NoError(t, writer.WriteHeader(&tar.Header{
		Name: ContractProgramArtifact,
		Mode: 0o644,
		Size: int64(len(artifact)),
	}))
	_, err := writer.Write(artifact)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}

func emptyMetadataTar(t *testing.T) []byte {
	t.Helper()
	buffer := bytes.NewBuffer(nil)
	writer := tar.NewWriter(buffer)
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}
