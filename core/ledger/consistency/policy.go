/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consistency

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

// Config points a peer at an offline-generated consistency manifest. An empty
// path keeps upstream-compatible behavior: all unlabelled state is implicitly
// normal and no additional metadata writes are generated.
type Config struct {
	ManifestPath string
}

// Manifest is the first version of the offline/manual per-state policy format.
// Rules are deterministic and independent of declaration order.
type Manifest struct {
	Version      int    `yaml:"version"`
	DefaultLevel string `yaml:"defaultLevel"`
	Rules        []Rule `yaml:"rules"`
}

// Rule classifies either one exact key, all keys sharing a prefix, or every key
// in a namespace. Exact rules take precedence over prefix rules; the longest
// matching prefix takes precedence over a namespace rule.
type Rule struct {
	Namespace     string  `yaml:"namespace"`
	Key           string  `yaml:"key,omitempty"`
	KeyPrefix     string  `yaml:"keyPrefix,omitempty"`
	Level         string  `yaml:"level"`
	TierThreshold *uint64 `yaml:"tierThreshold,omitempty"`
}

type prefixRule struct {
	prefix        string
	policySetting policySetting
}

type policySetting struct {
	level         Level
	tierThreshold uint64
	hasThreshold  bool
}

type policyKey struct {
	namespace string
	key       string
}

// Policy is an immutable, validated state-classification policy.
type Policy struct {
	defaultLevel     Level
	exactRules       map[policyKey]policySetting
	prefixRules      map[string][]prefixRule
	namespaceRules   map[string]policySetting
	manifestHash     string
	manifestPath     string
	hasExplicitRules bool
}

// LoadPolicy loads and validates a policy from Config.
func LoadPolicy(config *Config) (*Policy, error) {
	if config == nil || config.ManifestPath == "" {
		return NewPolicy(nil)
	}

	contents, err := os.ReadFile(config.ManifestPath)
	if err != nil {
		return nil, fmt.Errorf("read state consistency manifest %q: %w", config.ManifestPath, err)
	}

	manifest := &Manifest{}
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(manifest); err != nil {
		return nil, fmt.Errorf("parse state consistency manifest %q: %w", config.ManifestPath, err)
	}

	policy, err := NewPolicy(manifest)
	if err != nil {
		return nil, fmt.Errorf("validate state consistency manifest %q: %w", config.ManifestPath, err)
	}
	hash := sha256.Sum256(contents)
	policy.manifestHash = fmt.Sprintf("%x", hash[:])
	policy.manifestPath = config.ManifestPath
	return policy, nil
}

// NewPolicy validates and compiles a manifest. A nil manifest creates the
// upstream-compatible implicit-normal policy.
func NewPolicy(manifest *Manifest) (*Policy, error) {
	policy := &Policy{
		defaultLevel:   Normal,
		exactRules:     map[policyKey]policySetting{},
		prefixRules:    map[string][]prefixRule{},
		namespaceRules: map[string]policySetting{},
	}
	if manifest == nil {
		return policy, nil
	}
	if manifest.Version != 1 {
		return nil, fmt.Errorf("unsupported manifest version %d: expected 1", manifest.Version)
	}

	defaultLevel := manifest.DefaultLevel
	if defaultLevel == "" {
		defaultLevel = string(Normal)
	}
	parsedDefault, err := ParseLevel(defaultLevel)
	if err != nil {
		return nil, fmt.Errorf("invalid defaultLevel: %w", err)
	}
	if parsedDefault != Normal {
		return nil, fmt.Errorf(
			"defaultLevel must be %q in manifest version 1; classify application namespaces explicitly",
			Normal,
		)
	}
	policy.defaultLevel = parsedDefault

	seenPrefixes := map[policyKey]struct{}{}
	for index, rule := range manifest.Rules {
		level, err := ParseLevel(rule.Level)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", index, err)
		}
		if rule.Namespace == "" {
			return nil, fmt.Errorf("rule %d: namespace must not be empty", index)
		}
		if rule.Key != "" && rule.KeyPrefix != "" {
			return nil, fmt.Errorf("rule %d: key and keyPrefix are mutually exclusive", index)
		}
		setting := policySetting{level: level}
		if rule.TierThreshold != nil {
			if level != Relaxed {
				return nil, fmt.Errorf("rule %d: tierThreshold is only valid for relaxed state", index)
			}
			if *rule.TierThreshold == 0 {
				return nil, fmt.Errorf("rule %d: tierThreshold must be positive", index)
			}
			setting.tierThreshold = *rule.TierThreshold
			setting.hasThreshold = true
		}

		switch {
		case rule.Key != "":
			key := policyKey{namespace: rule.Namespace, key: rule.Key}
			if _, exists := policy.exactRules[key]; exists {
				return nil, fmt.Errorf("rule %d: duplicate exact rule for namespace %q key %q", index, rule.Namespace, rule.Key)
			}
			policy.exactRules[key] = setting

		case rule.KeyPrefix != "":
			key := policyKey{namespace: rule.Namespace, key: rule.KeyPrefix}
			if _, exists := seenPrefixes[key]; exists {
				return nil, fmt.Errorf("rule %d: duplicate prefix rule for namespace %q prefix %q", index, rule.Namespace, rule.KeyPrefix)
			}
			seenPrefixes[key] = struct{}{}
			policy.prefixRules[rule.Namespace] = append(
				policy.prefixRules[rule.Namespace],
				prefixRule{prefix: rule.KeyPrefix, policySetting: setting},
			)

		default:
			if _, exists := policy.namespaceRules[rule.Namespace]; exists {
				return nil, fmt.Errorf("rule %d: duplicate namespace rule for %q", index, rule.Namespace)
			}
			policy.namespaceRules[rule.Namespace] = setting
		}
	}

	for namespace := range policy.prefixRules {
		rules := policy.prefixRules[namespace]
		sort.Slice(rules, func(i, j int) bool {
			if len(rules[i].prefix) == len(rules[j].prefix) {
				return rules[i].prefix < rules[j].prefix
			}
			return len(rules[i].prefix) > len(rules[j].prefix)
		})
		policy.prefixRules[namespace] = rules
	}
	policy.hasExplicitRules = len(manifest.Rules) > 0
	return policy, nil
}

// Resolve returns the level for a public world-state key and whether an
// explicit rule produced it. An unlabelled key is implicitly normal.
func (p *Policy) Resolve(namespace, key string) (Level, bool) {
	setting, explicit := p.resolve(namespace, key)
	return setting.level, explicit
}

// ResolveTierThreshold returns the fixed preventive-synchronization threshold
// selected by the same exact-key/longest-prefix/namespace precedence as
// Resolve. A false result means that tier-triggered synchronization is disabled
// for this key.
func (p *Policy) ResolveTierThreshold(namespace, key string) (uint64, bool) {
	setting, explicit := p.resolve(namespace, key)
	if !explicit || setting.level != Relaxed || !setting.hasThreshold {
		return 0, false
	}
	return setting.tierThreshold, true
}

func (p *Policy) resolve(namespace, key string) (policySetting, bool) {
	if p == nil {
		return policySetting{level: Normal}, false
	}
	if setting, ok := p.exactRules[policyKey{namespace: namespace, key: key}]; ok {
		return setting, true
	}
	for _, rule := range p.prefixRules[namespace] {
		if len(key) >= len(rule.prefix) && key[:len(rule.prefix)] == rule.prefix {
			return rule.policySetting, true
		}
	}
	if setting, ok := p.namespaceRules[namespace]; ok {
		return setting, true
	}
	return policySetting{level: p.defaultLevel}, false
}

// ManifestHash is the SHA-256 hash of the raw manifest bytes.
func (p *Policy) ManifestHash() string {
	if p == nil {
		return ""
	}
	return p.manifestHash
}

// ManifestPath returns the manifest path used to construct the policy.
func (p *Policy) ManifestPath() string {
	if p == nil {
		return ""
	}
	return p.manifestPath
}

// Enabled reports whether the policy contains at least one explicit rule.
func (p *Policy) Enabled() bool {
	return p != nil && p.hasExplicitRules
}
