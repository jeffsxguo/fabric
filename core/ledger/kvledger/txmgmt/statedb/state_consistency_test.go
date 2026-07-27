/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statedb

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	stateconsistency "github.com/hyperledger/fabric/core/ledger/consistency"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statemetadata"
	"github.com/stretchr/testify/require"
)

func TestVersionedValueStateConsistencyLevel(t *testing.T) {
	var missing *VersionedValue
	level, err := missing.StateConsistencyLevel()
	require.NoError(t, err)
	require.Equal(t, stateconsistency.Normal, level)

	implicit := &VersionedValue{Value: []byte("value")}
	level, err = implicit.StateConsistencyLevel()
	require.NoError(t, err)
	require.Equal(t, stateconsistency.Normal, level)

	metadata, err := statemetadata.Serialize([]*kvrwset.KVMetadataEntry{
		{Name: stateconsistency.MetadataKey, Value: []byte(stateconsistency.Relaxed)},
	})
	require.NoError(t, err)
	explicit := &VersionedValue{Value: []byte("value"), Metadata: metadata}
	level, err = explicit.StateConsistencyLevel()
	require.NoError(t, err)
	require.Equal(t, stateconsistency.Relaxed, level)
}
