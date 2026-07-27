/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consistency

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImplicitNormalPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy func() (*Policy, error)
	}{
		{
			name: "no manifest",
			policy: func() (*Policy, error) {
				return NewPolicy(nil)
			},
		},
		{
			name: "empty manifest default",
			policy: func() (*Policy, error) {
				return NewPolicy(&Manifest{Version: 1})
			},
		},
		{
			name: "nil receiver",
			policy: func() (*Policy, error) {
				return nil, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := test.policy()
			require.NoError(t, err)
			level, explicit := policy.Resolve("legacy-chaincode", "legacy-key")
			require.Equal(t, Normal, level)
			require.False(t, explicit)
		})
	}
}

func TestPolicyPrecedence(t *testing.T) {
	policy, err := NewPolicy(&Manifest{
		Version:      1,
		DefaultLevel: "normal",
		Rules: []Rule{
			{Namespace: "basic", Level: "strong"},
			{Namespace: "basic", KeyPrefix: "asset", Level: "normal"},
			{Namespace: "basic", KeyPrefix: "asset-private", Level: "relaxed"},
			{Namespace: "basic", Key: "asset1", Level: "relaxed"},
		},
	})
	require.NoError(t, err)
	require.True(t, policy.Enabled())

	tests := []struct {
		namespace string
		key       string
		level     Level
		explicit  bool
	}{
		{"basic", "asset1", Relaxed, true},
		{"basic", "asset2", Normal, true},
		{"basic", "asset-private-1", Relaxed, true},
		{"basic", "other", Strong, true},
		{"other", "asset1", Normal, false},
	}
	for _, test := range tests {
		level, explicit := policy.Resolve(test.namespace, test.key)
		require.Equal(t, test.level, level)
		require.Equal(t, test.explicit, explicit)
	}
}

func TestLoadPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
version: 1
defaultLevel: normal
rules:
  - namespace: basic
    keyPrefix: "relaxed:"
    level: relaxed
`), 0o600))

	policy, err := LoadPolicy(&Config{ManifestPath: path})
	require.NoError(t, err)
	require.NotEmpty(t, policy.ManifestHash())
	require.Equal(t, path, policy.ManifestPath())
	level, explicit := policy.Resolve("basic", "relaxed:oracle")
	require.Equal(t, Relaxed, level)
	require.True(t, explicit)
}

func TestLoadPolicyRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
version: 1
defaultLevel: normal
unexpectedField: true
`), 0o600))

	_, err := LoadPolicy(&Config{ManifestPath: path})
	require.ErrorContains(t, err, "field unexpectedField not found")
}

func TestInvalidPolicies(t *testing.T) {
	tests := []struct {
		name     string
		manifest *Manifest
		error    string
	}{
		{
			name:     "version",
			manifest: &Manifest{Version: 2},
			error:    "unsupported manifest version 2",
		},
		{
			name:     "default",
			manifest: &Manifest{Version: 1, DefaultLevel: "strong"},
			error:    `defaultLevel must be "normal"`,
		},
		{
			name: "selectors",
			manifest: &Manifest{Version: 1, Rules: []Rule{
				{Namespace: "basic", Key: "key", KeyPrefix: "prefix", Level: "normal"},
			}},
			error: "key and keyPrefix are mutually exclusive",
		},
		{
			name: "duplicate namespace",
			manifest: &Manifest{Version: 1, Rules: []Rule{
				{Namespace: "basic", Level: "strong"},
				{Namespace: "basic", Level: "normal"},
			}},
			error: "duplicate namespace rule",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewPolicy(test.manifest)
			require.ErrorContains(t, err, test.error)
		})
	}
}
