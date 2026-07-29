/*
Copyright 2026 GraND Authors.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	msp "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/protobuf/proto"
)

const grandPassiveDivergenceMarker = "GRAND_PASSIVE_DIVERGENCE:"

type grandPassiveResponseGroup struct {
	CanonicalResponse string   `json:"canonicalResponse"`
	MSPIDs            []string `json:"mspIds"`
	PayloadHash       string   `json:"payloadHash"`
}

// grandPassiveDivergenceSummary returns a deterministic description of exact
// ProposalResponsePayload groups when relaxed observations accompanied a
// divergent endorsement round. It is diagnostic output for the analyzer-free
// recovery orchestrator; endorsement-policy evaluation remains outside this
// helper.
func grandPassiveDivergenceSummary(responses []*pb.ProposalResponse) string {
	type group struct {
		response string
		msps     map[string]struct{}
		hash     string
	}
	groups := map[string]*group{}
	hasRelaxedEvidence := false
	for _, response := range responses {
		if response == nil || response.Response == nil ||
			response.Response.Status < 200 || response.Response.Status >= 400 ||
			response.Endorsement == nil ||
			!strings.HasPrefix(response.Response.Message, protoutil.GrandRelaxedEvidenceMessagePrefix) {
			return ""
		}
		hasRelaxedEvidence = true
		key := string(response.Payload)
		current := groups[key]
		if current == nil {
			digest := sha256.Sum256(response.Payload)
			current = &group{
				response: grandCanonicalResponse(response),
				msps:     map[string]struct{}{},
				hash:     hex.EncodeToString(digest[:]),
			}
			groups[key] = current
		}
		if mspID := grandResponseMSPID(response); mspID != "" {
			current.msps[mspID] = struct{}{}
		}
	}
	if !hasRelaxedEvidence || len(groups) <= 1 {
		return ""
	}

	summary := make([]grandPassiveResponseGroup, 0, len(groups))
	for _, current := range groups {
		mspIDs := make([]string, 0, len(current.msps))
		for mspID := range current.msps {
			mspIDs = append(mspIDs, mspID)
		}
		sort.Strings(mspIDs)
		summary = append(summary, grandPassiveResponseGroup{
			CanonicalResponse: current.response,
			MSPIDs:            mspIDs,
			PayloadHash:       current.hash,
		})
	}
	sort.Slice(summary, func(i, j int) bool {
		if summary[i].CanonicalResponse == summary[j].CanonicalResponse {
			return summary[i].PayloadHash < summary[j].PayloadHash
		}
		return summary[i].CanonicalResponse < summary[j].CanonicalResponse
	})
	encoded, err := json.Marshal(summary)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func grandCanonicalResponse(response *pb.ProposalResponse) string {
	if response != nil {
		payload, err := protoutil.UnmarshalProposalResponsePayload(response.Payload)
		if err == nil {
			action, actionErr := protoutil.UnmarshalChaincodeAction(payload.Extension)
			if actionErr == nil && action.Response != nil {
				return string(action.Response.Payload)
			}
		}
		if response.Response != nil {
			return string(response.Response.Payload)
		}
	}
	return ""
}

func grandResponseMSPID(response *pb.ProposalResponse) string {
	if response == nil || response.Endorsement == nil {
		return ""
	}
	identity := &msp.SerializedIdentity{}
	if err := proto.Unmarshal(response.Endorsement.Endorser, identity); err != nil {
		return ""
	}
	return identity.Mspid
}
