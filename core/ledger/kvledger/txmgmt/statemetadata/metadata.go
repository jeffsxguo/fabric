/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemetadata

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	stateconsistency "github.com/hyperledger/fabric/core/ledger/consistency"
	"google.golang.org/protobuf/proto"
)

// Serialize serializes metadata entries for storing in statedb
func Serialize(metadataEntries []*kvrwset.KVMetadataEntry) ([]byte, error) {
	metadata := &kvrwset.KVMetadataWrite{Entries: metadataEntries}
	return proto.Marshal(metadata)
}

// Deserialize deserializes metadata bytes from statedb
func Deserialize(metadataBytes []byte) (map[string][]byte, error) {
	if metadataBytes == nil {
		return nil, nil
	}
	metadata := &kvrwset.KVMetadataWrite{}
	if err := proto.Unmarshal(metadataBytes, metadata); err != nil {
		return nil, err
	}
	m := make(map[string][]byte, len(metadata.Entries))
	for _, metadataEntry := range metadata.Entries {
		m[metadataEntry.Name] = metadataEntry.Value
	}
	return m, nil
}

// DeserializeConsistencyLevel returns the GraND consistency level stored in
// serialized state metadata. Missing metadata is interpreted as normal.
func DeserializeConsistencyLevel(metadataBytes []byte) (stateconsistency.Level, error) {
	metadata, err := Deserialize(metadataBytes)
	if err != nil {
		return "", err
	}
	return stateconsistency.FromMetadata(metadata)
}
