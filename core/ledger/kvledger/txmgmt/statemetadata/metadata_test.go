/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemetadata

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	stateconsistency "github.com/hyperledger/fabric/core/ledger/consistency"
	"github.com/stretchr/testify/require"
)

func TestSerializeDeSerialize(t *testing.T) {
	metadataEntries := []*kvrwset.KVMetadataEntry{}
	metadataEntries = append(metadataEntries, &kvrwset.KVMetadataEntry{Name: "metadata_1", Value: []byte("metadata_value_1")})
	metadataEntries = append(metadataEntries, &kvrwset.KVMetadataEntry{Name: "metadata_2", Value: []byte("metadata_value_2")})
	metadataEntries = append(metadataEntries, &kvrwset.KVMetadataEntry{Name: "metadata_3", Value: []byte("metadata_value_3")})

	serializedMetadata, err := Serialize(metadataEntries)
	require.NoError(t, err)
	deserializedMetadata, err := Deserialize(serializedMetadata)
	require.NoError(t, err)

	expectedMetadata := map[string][]byte{
		"metadata_1": []byte("metadata_value_1"),
		"metadata_2": []byte("metadata_value_2"),
		"metadata_3": []byte("metadata_value_3"),
	}
	require.Equal(t, expectedMetadata, deserializedMetadata)
}

func TestDeserializeConsistencyLevel(t *testing.T) {
	level, err := DeserializeConsistencyLevel(nil)
	require.NoError(t, err)
	require.Equal(t, stateconsistency.Normal, level)

	serializedMetadata, err := Serialize([]*kvrwset.KVMetadataEntry{
		{Name: stateconsistency.MetadataKey, Value: []byte(stateconsistency.Normal)},
	})
	require.NoError(t, err)
	level, err = DeserializeConsistencyLevel(serializedMetadata)
	require.NoError(t, err)
	require.Equal(t, stateconsistency.Normal, level)
}
