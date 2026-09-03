/*
Copyright 2026 GraND Authors.

SPDX-License-Identifier: Apache-2.0
*/

package consistency

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
)

const (
	// ContractProgramArtifact is the canonical location of an offline analysis
	// result inside a Fabric chaincode code package.
	ContractProgramArtifact = "META-INF/grand/consistency.json"

	ContractProgramSchemaVersion = 1
	SignedIntegerLevelEncoding   = "signed-integer-v1"

	IdentityChange  = "identity"
	IncrementChange = "increment"

	// NumericMetadataKey stores the signed integer representation requested by
	// the deployed contract program. MetadataKey remains present as the
	// human-readable/backward-compatible strong|normal|relaxed label.
	NumericMetadataKey = "GRAND_CONSISTENCY_LEVEL_INT"

	StrongNumericLevel int64 = -1
	NormalNumericLevel int64 = 0

	maxContractProgramSize = 1 << 20
)

// ContractProgram is the deployable result of offline contract analysis for
// the first online integration. The threshold is deliberately carried and
// persisted now, but is not used to trigger synchronization in this version.
type ContractProgram struct {
	SchemaVersion        int    `json:"schemaVersion"`
	LevelEncoding        string `json:"levelEncoding"`
	InitialLevel         int64  `json:"initialLevel"`
	ChangeFunction       string `json:"changeFunction"`
	RelaxedTierThreshold uint64 `json:"relaxedTierThreshold"`
}

// DeployedContractProgram binds an analysis result to the chaincode package
// that supplied it through the Fabric lifecycle.
type DeployedContractProgram struct {
	ChaincodeName string          `json:"chaincodeName"`
	Version       string          `json:"version"`
	PackageID     string          `json:"packageId"`
	ArtifactHash  string          `json:"artifactHash"`
	Program       ContractProgram `json:"program"`
}

// ParseContractProgram strictly parses and validates one offline result.
func ParseContractProgram(contents []byte) (*ContractProgram, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	program := &ContractProgram{}
	if err := decoder.Decode(program); err != nil {
		return nil, fmt.Errorf("parse GraND contract consistency program: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	if err := program.Validate(); err != nil {
		return nil, err
	}
	return program, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("parse GraND contract consistency program: multiple JSON values")
		}
		return fmt.Errorf("parse GraND contract consistency program: trailing data: %w", err)
	}
	return nil
}

// Validate checks the deliberately small first-version transition language.
func (p *ContractProgram) Validate() error {
	if p == nil {
		return fmt.Errorf("GraND contract consistency program is nil")
	}
	if p.SchemaVersion != ContractProgramSchemaVersion {
		return fmt.Errorf(
			"unsupported GraND contract consistency program schema %d: expected %d",
			p.SchemaVersion,
			ContractProgramSchemaVersion,
		)
	}
	if p.LevelEncoding != SignedIntegerLevelEncoding {
		return fmt.Errorf(
			"unsupported GraND consistency level encoding %q: expected %q",
			p.LevelEncoding,
			SignedIntegerLevelEncoding,
		)
	}
	if p.InitialLevel != NormalNumericLevel {
		return fmt.Errorf(
			"unsupported initial consistency level %d: schema 1 requires normal (%d)",
			p.InitialLevel,
			NormalNumericLevel,
		)
	}
	switch p.ChangeFunction {
	case IdentityChange, IncrementChange:
	default:
		return fmt.Errorf(
			"unsupported consistency change function %q: expected %q or %q",
			p.ChangeFunction,
			IdentityChange,
			IncrementChange,
		)
	}
	if p.RelaxedTierThreshold == 0 {
		return fmt.Errorf("relaxedTierThreshold must be positive")
	}
	return nil
}

// Apply evaluates the deployed change function over the maximum supplied
// input level. Empty input starts at InitialLevel. Integer ordering is the
// consistency lattice requested for this stage: -1 strong, 0 normal, and
// positive values relaxed tiers.
func (p *ContractProgram) Apply(inputs []int64) (int64, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	base := p.InitialLevel
	if len(inputs) > 0 {
		base = inputs[0]
		for _, input := range inputs[1:] {
			if input < StrongNumericLevel {
				return 0, fmt.Errorf("invalid consistency level %d: minimum is %d", input, StrongNumericLevel)
			}
			if input > base {
				base = input
			}
		}
		if base < StrongNumericLevel {
			return 0, fmt.Errorf("invalid consistency level %d: minimum is %d", base, StrongNumericLevel)
		}
	}
	if p.ChangeFunction == IdentityChange {
		return base, nil
	}
	if base == math.MaxInt64 {
		return 0, fmt.Errorf("consistency level overflow: %d + 1", base)
	}
	return base + 1, nil
}

// ExtractContractProgram reads the offline result from the metadata tar that
// Fabric lifecycle already delivers to HandleChaincodeDeploy.
func ExtractContractProgram(metadataTar []byte) (*ContractProgram, []byte, bool, error) {
	reader := tar.NewReader(bytes.NewReader(metadataTar))
	var artifact []byte
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Fabric historically suppresses malformed CouchDB artifact tar
			// errors at deployment. Preserve that behavior when no GraND entry
			// was observed; a package produced by the normal metadata provider
			// has already been strictly validated during installation.
			if artifact == nil {
				return nil, nil, false, nil
			}
			return nil, nil, false, fmt.Errorf("read chaincode deployment metadata: %w", err)
		}
		if header.Name != ContractProgramArtifact {
			continue
		}
		if artifact != nil {
			return nil, nil, false, fmt.Errorf("duplicate %s in chaincode package", ContractProgramArtifact)
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return nil, nil, false, fmt.Errorf("%s is not a regular file", ContractProgramArtifact)
		}
		if header.Size < 0 || header.Size > maxContractProgramSize {
			return nil, nil, false, fmt.Errorf(
				"%s exceeds the %d byte limit",
				ContractProgramArtifact,
				maxContractProgramSize,
			)
		}
		artifact, err = io.ReadAll(io.LimitReader(reader, maxContractProgramSize+1))
		if err != nil {
			return nil, nil, false, fmt.Errorf("read %s: %w", ContractProgramArtifact, err)
		}
		if len(artifact) > maxContractProgramSize {
			return nil, nil, false, fmt.Errorf(
				"%s exceeds the %d byte limit",
				ContractProgramArtifact,
				maxContractProgramSize,
			)
		}
	}
	if artifact == nil {
		return nil, nil, false, nil
	}
	program, err := ParseContractProgram(artifact)
	if err != nil {
		return nil, nil, false, err
	}
	return program, artifact, true, nil
}

// NumericLevelFromMetadata returns the integer level stored on canonical
// state. Old string-only metadata remains readable.
func NumericLevelFromMetadata(metadata map[string][]byte) (int64, error) {
	if encoded, ok := metadata[NumericMetadataKey]; ok {
		level, err := strconv.ParseInt(string(encoded), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid numeric consistency level %q: %w", encoded, err)
		}
		if level < StrongNumericLevel {
			return 0, fmt.Errorf("invalid numeric consistency level %d: minimum is %d", level, StrongNumericLevel)
		}
		return level, nil
	}
	level, err := FromMetadata(metadata)
	if err != nil {
		return 0, err
	}
	switch level {
	case Strong:
		return StrongNumericLevel, nil
	case Normal:
		return NormalNumericLevel, nil
	case Relaxed:
		// Old canonical metadata did not carry a tier. Tier one is the least
		// relaxed value representable by the signed-integer model.
		return 1, nil
	default:
		return 0, fmt.Errorf("unsupported consistency level %q", level)
	}
}

// WithNumericLevel preserves unrelated state metadata and writes both the
// integer encoding and the existing categorical compatibility label.
func WithNumericLevel(metadata map[string][]byte, numericLevel int64) (map[string][]byte, error) {
	if numericLevel < StrongNumericLevel {
		return nil, fmt.Errorf("invalid numeric consistency level %d: minimum is %d", numericLevel, StrongNumericLevel)
	}
	level := Relaxed
	if numericLevel == StrongNumericLevel {
		level = Strong
	} else if numericLevel == NormalNumericLevel {
		level = Normal
	}
	result, err := WithLevel(metadata, level)
	if err != nil {
		return nil, err
	}
	result[NumericMetadataKey] = []byte(strconv.FormatInt(numericLevel, 10))
	return result, nil
}

type stagedContractProgram struct {
	name    string
	record  *DeployedContractProgram
	removal bool
}

// ContractProgramRegistry persists lifecycle-deployed analysis results per
// channel and exposes an immutable snapshot to concurrent simulations.
type ContractProgramRegistry struct {
	db       *leveldbhelper.DBHandle
	mutex    sync.RWMutex
	programs map[string]*DeployedContractProgram
	staged   map[string]stagedContractProgram
}

func NewContractProgramRegistry(db *leveldbhelper.DBHandle) (*ContractProgramRegistry, error) {
	registry := &ContractProgramRegistry{
		db:       db,
		programs: map[string]*DeployedContractProgram{},
		staged:   map[string]stagedContractProgram{},
	}
	if db == nil {
		return registry, nil
	}
	iterator, err := db.GetIterator(nil, nil)
	if err != nil {
		return nil, err
	}
	defer iterator.Release()
	for iterator.Next() {
		record := &DeployedContractProgram{}
		if err := json.Unmarshal(iterator.Value(), record); err != nil {
			return nil, fmt.Errorf("decode deployed consistency program %q: %w", iterator.Key(), err)
		}
		if err := record.Program.Validate(); err != nil {
			return nil, fmt.Errorf("validate deployed consistency program %q: %w", iterator.Key(), err)
		}
		registry.programs[string(iterator.Key())] = cloneDeployedContractProgram(record)
	}
	if err := iterator.Error(); err != nil {
		return nil, err
	}
	return registry, nil
}

// StageDeployment records one lifecycle callback. Missing metadata removes an
// earlier program only after ChaincodeDeployDone reports success.
func (r *ContractProgramRegistry) StageDeployment(
	chaincodeName,
	version,
	packageID string,
	metadataTar []byte,
) (*DeployedContractProgram, bool, error) {
	if chaincodeName == "" {
		return nil, false, fmt.Errorf("chaincode name is required for a consistency program deployment")
	}
	program, artifact, found, err := ExtractContractProgram(metadataTar)
	if err != nil {
		return nil, false, err
	}
	staged := stagedContractProgram{name: chaincodeName, removal: !found}
	if found {
		hash := sha256.Sum256(artifact)
		staged.record = &DeployedContractProgram{
			ChaincodeName: chaincodeName,
			Version:       version,
			PackageID:     packageID,
			ArtifactHash:  fmt.Sprintf("%x", hash[:]),
			Program:       *program,
		}
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.staged[chaincodeName] = staged
	return cloneDeployedContractProgram(staged.record), found, nil
}

// DeploymentDone atomically publishes every staged callback after a
// successful lifecycle operation, or discards them after failure.
func (r *ContractProgramRegistry) DeploymentDone(succeeded bool) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !succeeded {
		r.staged = map[string]stagedContractProgram{}
		return nil
	}
	if r.db != nil {
		batch := r.db.NewUpdateBatch()
		for name, staged := range r.staged {
			if staged.removal {
				batch.Delete([]byte(name))
				continue
			}
			encoded, err := json.Marshal(staged.record)
			if err != nil {
				return err
			}
			batch.Put([]byte(name), encoded)
		}
		if err := r.db.WriteBatch(batch, true); err != nil {
			return err
		}
	}
	for name, staged := range r.staged {
		if staged.removal {
			delete(r.programs, name)
			continue
		}
		r.programs[name] = cloneDeployedContractProgram(staged.record)
	}
	r.staged = map[string]stagedContractProgram{}
	return nil
}

// Lookup returns a copy of the currently deployed program for a namespace.
func (r *ContractProgramRegistry) Lookup(namespace string) (*DeployedContractProgram, bool) {
	if r == nil {
		return nil, false
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	record, ok := r.programs[namespace]
	if !ok {
		return nil, false
	}
	return cloneDeployedContractProgram(record), true
}

func cloneDeployedContractProgram(record *DeployedContractProgram) *DeployedContractProgram {
	if record == nil {
		return nil
	}
	clone := *record
	return &clone
}
