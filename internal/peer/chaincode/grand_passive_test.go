/*
Copyright 2026 GraND Authors.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"encoding/json"
	"testing"

	msp "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestGrandPassiveDivergenceSummary(t *testing.T) {
	response := func(payload, canonical, mspID string) *pb.ProposalResponse {
		identity, err := proto.Marshal(&msp.SerializedIdentity{Mspid: mspID, IdBytes: []byte(mspID)})
		require.NoError(t, err)
		return &pb.ProposalResponse{
			Payload: []byte(payload),
			Response: &pb.Response{
				Status:  200,
				Message: protoutil.GrandRelaxedEvidenceMessagePrefix + "evidence",
				Payload: []byte(canonical),
			},
			Endorsement: &pb.Endorsement{Endorser: identity},
		}
	}

	summary := grandPassiveDivergenceSummary([]*pb.ProposalResponse{
		response("execute-payload", "execute", "Org4MSP"),
		response("hold-payload", "hold", "Org2MSP"),
		response("execute-payload", "execute", "Org3MSP"),
		response("hold-payload", "hold", "Org1MSP"),
	})
	require.NotEmpty(t, summary)
	groups := []grandPassiveResponseGroup{}
	require.NoError(t, json.Unmarshal([]byte(summary), &groups))
	require.Len(t, groups, 2)
	require.Equal(t, "execute", groups[0].CanonicalResponse)
	require.Equal(t, []string{"Org3MSP", "Org4MSP"}, groups[0].MSPIDs)
	require.Equal(t, "hold", groups[1].CanonicalResponse)
	require.Equal(t, []string{"Org1MSP", "Org2MSP"}, groups[1].MSPIDs)

	require.Empty(t, grandPassiveDivergenceSummary([]*pb.ProposalResponse{
		response("same", "hold", "Org1MSP"),
		response("same", "hold", "Org2MSP"),
	}))
}
