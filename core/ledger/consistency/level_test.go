/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consistency

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLevelsAndMetadata(t *testing.T) {
	for _, level := range []Level{Strong, Normal, Relaxed} {
		parsed, err := ParseLevel(string(level))
		require.NoError(t, err)
		require.Equal(t, level, parsed)
	}
	_, err := ParseLevel("eventual")
	require.EqualError(
		t,
		err,
		`invalid state consistency level "eventual": expected "strong", "normal", or "relaxed"`,
	)

	level, err := FromMetadata(nil)
	require.NoError(t, err)
	require.Equal(t, Normal, level)

	original := map[string][]byte{"VALIDATION_PARAMETER": []byte("policy")}
	metadata, err := WithLevel(original, Relaxed)
	require.NoError(t, err)
	require.Equal(t, []byte("policy"), metadata["VALIDATION_PARAMETER"])
	require.Equal(t, []byte("relaxed"), metadata[MetadataKey])
	require.NotContains(t, original, MetadataKey)

	level, err = FromMetadata(metadata)
	require.NoError(t, err)
	require.Equal(t, Relaxed, level)

	metadata[MetadataKey] = []byte("unknown")
	_, err = FromMetadata(metadata)
	require.ErrorContains(t, err, "invalid state consistency level")
}

type metadataState struct {
	metadata map[string][]byte
}

func (s *metadataState) GetStateMetadata(_, _ string) (map[string][]byte, error) {
	return s.metadata, nil
}

func (s *metadataState) SetStateMetadata(_, _ string, metadata map[string][]byte) error {
	s.metadata = metadata
	return nil
}

func TestStateLevelReaderWriter(t *testing.T) {
	state := &metadataState{metadata: map[string][]byte{"other": []byte("value")}}
	require.NoError(t, SetStateLevel(state, state, "basic", "asset1", Normal))

	level, err := GetStateLevel(state, "basic", "asset1")
	require.NoError(t, err)
	require.Equal(t, Normal, level)
	require.Equal(t, []byte("value"), state.metadata["other"])
}
