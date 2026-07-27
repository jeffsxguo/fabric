/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package consistency defines the canonical per-key consistency classification
// used by GraND. The classification is stored as an entry in Fabric's existing
// state metadata, so it is versioned, endorsed, ordered, snapshotted, and
// committed atomically with the state value.
package consistency

import "fmt"

const (
	// MetadataKey is the state metadata entry used to persist a consistency
	// level. It intentionally does not reuse Fabric's validation-parameter
	// metadata entry; both entries can coexist on the same key.
	MetadataKey = "GRAND_CONSISTENCY_LEVEL"
)

// Level is the consistency classification of a public world-state key.
type Level string

const (
	Strong  Level = "strong"
	Normal  Level = "normal"
	Relaxed Level = "relaxed"
)

// ParseLevel validates and returns a canonical consistency level.
func ParseLevel(value string) (Level, error) {
	level := Level(value)
	switch level {
	case Strong, Normal, Relaxed:
		return level, nil
	default:
		return "", fmt.Errorf(
			"invalid state consistency level %q: expected %q, %q, or %q",
			value,
			Strong,
			Normal,
			Relaxed,
		)
	}
}

// ParseLevelBytes validates a consistency level stored in state metadata.
func ParseLevelBytes(value []byte) (Level, error) {
	return ParseLevel(string(value))
}

// FromMetadata returns the consistency level in metadata. State created by an
// unmodified Fabric peer has no GraND metadata; it is interpreted as normal for
// backward compatibility. Normal state still uses standard Fabric execution
// until GraND's online execution semantics are enabled.
func FromMetadata(metadata map[string][]byte) (Level, error) {
	value, ok := metadata[MetadataKey]
	if !ok {
		return Normal, nil
	}
	return ParseLevelBytes(value)
}

// WithLevel returns a copy of metadata containing the canonical consistency
// entry. Existing entries, including key-level endorsement parameters, are
// preserved.
func WithLevel(metadata map[string][]byte, level Level) (map[string][]byte, error) {
	canonical, err := ParseLevel(string(level))
	if err != nil {
		return nil, err
	}

	result := cloneMetadata(metadata)
	result[MetadataKey] = []byte(canonical)
	return result, nil
}

// MetadataReader is implemented by ledger.QueryExecutor, ledger.TxSimulator,
// and commit-time transaction state adapters.
type MetadataReader interface {
	GetStateMetadata(namespace, key string) (map[string][]byte, error)
}

// MetadataWriter is implemented by ledger.TxSimulator and commit-time
// transaction state adapters.
type MetadataWriter interface {
	SetStateMetadata(namespace, key string, metadata map[string][]byte) error
}

// GetStateLevel reads a key's consistency classification through an existing
// Fabric metadata reader.
func GetStateLevel(reader MetadataReader, namespace, key string) (Level, error) {
	metadata, err := reader.GetStateMetadata(namespace, key)
	if err != nil {
		return "", err
	}
	return FromMetadata(metadata)
}

// SetStateLevel adds a consistency classification to a simulated or
// commit-time state update. Callers should write the state value in the same
// transaction; Fabric ignores a metadata-only write for a key that does not
// exist.
func SetStateLevel(
	reader MetadataReader,
	writer MetadataWriter,
	namespace,
	key string,
	level Level,
) error {
	metadata, err := reader.GetStateMetadata(namespace, key)
	if err != nil {
		return err
	}
	metadata, err = WithLevel(metadata, level)
	if err != nil {
		return err
	}
	return writer.SetStateMetadata(namespace, key, metadata)
}

func cloneMetadata(metadata map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(metadata)+1)
	for key, value := range metadata {
		result[key] = append([]byte(nil), value...)
	}
	return result
}
