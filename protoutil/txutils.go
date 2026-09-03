/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoutil

import (
	"bytes"
	"crypto/sha256"
	b64 "encoding/base64"
	"encoding/json"
	"sort"
	"strings"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	mspproto "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	// GrandRelaxedEvidenceMessagePrefix marks an individually signed, peer-local
	// relaxed-state endorsement carried alongside a canonical proposal response.
	GrandRelaxedEvidenceMessagePrefix = "GRAND_LOCAL_STATE_ENDORSEMENT_V1:"
	// GrandRelaxedEvidenceTransientKey is retained in the ordered transaction as
	// an opaque evidence bundle. Unlike application transient data, validators
	// intentionally preserve and inspect this reserved entry.
	GrandRelaxedEvidenceTransientKey = "GRAND_RELAXED_EVIDENCE_V1"
	// GrandOrderedProposalGroupsMessagePrefix marks a GraND transaction that
	// retained multiple endorsed proposal-result groups in the ordered envelope.
	GrandOrderedProposalGroupsMessagePrefix = "GRAND_ORDERED_PROPOSAL_GROUPS_V1:"
	// GrandActiveSyncRequestMessage is the byte-identical chaincode response
	// marker that asks endorsers to certify the relaxed value they read.
	GrandActiveSyncRequestMessage = "GRAND_ACTIVE_SYNC_MEDIAN_JSON_PRICE_V1"
	// GrandActiveSyncResultMessagePrefix marks a client-facing result produced
	// only after the corresponding active-sync transaction has committed.
	GrandActiveSyncResultMessagePrefix = "GRAND_ACTIVE_SYNC_RESULT_V1:"
	GrandActiveSyncPurpose             = "active-sync"
	GrandLocalEndorsementDomain        = "GRAND_LOCAL_STATE_ENDORSEMENT_V1"
	GrandMedianJSONPriceAlgorithm      = "median-json-price-v1"
	GrandActiveSyncObservationQuorum   = 3
	grandRelaxedEvidenceBundleVersion  = 1
)

// GrandRelaxedEvidenceBundle is retained in the ordered transaction. Evidence
// contains peer-local signed observations; ProposalGroups preserves every exact
// divergent ProposalResponsePayload group and its endorsements; ActiveSync
// contains the deterministic aggregate that validators must recompute before
// accepting the transaction.
type GrandRelaxedEvidenceBundle struct {
	Version        int                    `json:"version"`
	Evidence       [][]byte               `json:"evidence"`
	ProposalGroups []GrandProposalGroup   `json:"proposalGroups,omitempty"`
	ActiveSync     *GrandActiveSyncResult `json:"activeSync,omitempty"`
}

// GrandProposalGroup is one exact result of executing the same proposal. The
// canonical action uses the 2/3 group while every group remains in this bundle
// so validators and later recovery logic observe what was actually endorsed.
type GrandProposalGroup struct {
	ProposalResponsePayload []byte                     `json:"proposalResponsePayload"`
	Endorsements            []GrandProposalEndorsement `json:"endorsements"`
}

type GrandProposalEndorsement struct {
	Endorser  []byte `json:"endorser"`
	Signature []byte `json:"signature"`
}

type GrandActiveSyncResult struct {
	Namespace string   `json:"namespace"`
	Key       string   `json:"key"`
	Value     []byte   `json:"value"`
	ValueHash []byte   `json:"valueHash"`
	Algorithm string   `json:"algorithm"`
	MSPIDs    []string `json:"mspIds"`
}

type grandRelaxedReadEvidence struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	ValueHash []byte `json:"valueHash"`
}

type grandRelaxedWriteEvidence struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value,omitempty"`
	ValueHash []byte `json:"valueHash"`
	Delete    bool   `json:"delete,omitempty"`
}

type grandRelaxedEvidencePayload struct {
	Domain              string                      `json:"domain"`
	ChannelID           string                      `json:"channelId"`
	TxID                string                      `json:"txId"`
	Purpose             string                      `json:"purpose"`
	ProposalHash        []byte                      `json:"proposalHash"`
	CanonicalResultHash []byte                      `json:"canonicalResultHash"`
	Reads               []grandRelaxedReadEvidence  `json:"reads,omitempty"`
	Writes              []grandRelaxedWriteEvidence `json:"writes,omitempty"`
}

type grandSignedRelaxedEvidence struct {
	Payload   grandRelaxedEvidencePayload `json:"payload"`
	Endorser  []byte                      `json:"endorser"`
	Signature []byte                      `json:"signature"`
}

// GetPayloads gets the underlying payload objects in a TransactionAction
func GetPayloads(txActions *peer.TransactionAction) (*peer.ChaincodeActionPayload, *peer.ChaincodeAction, error) {
	// TODO: pass in the tx type (in what follows we're assuming the
	// type is ENDORSER_TRANSACTION)
	ccPayload, err := UnmarshalChaincodeActionPayload(txActions.Payload)
	if err != nil {
		return nil, nil, err
	}

	if ccPayload.Action == nil || ccPayload.Action.ProposalResponsePayload == nil {
		return nil, nil, errors.New("no payload in ChaincodeActionPayload")
	}
	pRespPayload, err := UnmarshalProposalResponsePayload(ccPayload.Action.ProposalResponsePayload)
	if err != nil {
		return nil, nil, err
	}

	if pRespPayload.Extension == nil {
		return nil, nil, errors.New("response payload is missing extension")
	}

	respPayload, err := UnmarshalChaincodeAction(pRespPayload.Extension)
	if err != nil {
		return ccPayload, nil, err
	}
	return ccPayload, respPayload, nil
}

// GetEnvelopeFromBlock gets an envelope from a block's Data field.
func GetEnvelopeFromBlock(data []byte) (*common.Envelope, error) {
	// Block always begins with an envelope
	var err error
	env := &common.Envelope{}
	if err = proto.Unmarshal(data, env); err != nil {
		return nil, errors.Wrap(err, "error unmarshalling Envelope")
	}

	return env, nil
}

// CreateSignedEnvelope creates a signed envelope of the desired type, with
// marshaled dataMsg and signs it
func CreateSignedEnvelope(
	txType common.HeaderType,
	channelID string,
	signer Signer,
	dataMsg proto.Message,
	msgVersion int32,
	epoch uint64,
) (*common.Envelope, error) {
	return CreateSignedEnvelopeWithTLSBinding(txType, channelID, signer, dataMsg, msgVersion, epoch, nil)
}

// CreateSignedEnvelopeWithTLSBinding creates a signed envelope of the desired
// type, with marshaled dataMsg and signs it. It also includes a TLS cert hash
// into the channel header
func CreateSignedEnvelopeWithTLSBinding(
	txType common.HeaderType,
	channelID string,
	signer Signer,
	dataMsg proto.Message,
	msgVersion int32,
	epoch uint64,
	tlsCertHash []byte,
) (*common.Envelope, error) {
	payloadChannelHeader := MakeChannelHeader(txType, msgVersion, channelID, epoch)
	payloadChannelHeader.TlsCertHash = tlsCertHash
	var err error
	payloadSignatureHeader := &common.SignatureHeader{}

	if signer != nil {
		payloadSignatureHeader, err = NewSignatureHeader(signer)
		if err != nil {
			return nil, err
		}
	}

	if !dataMsg.ProtoReflect().IsValid() {
		return nil, errors.New("error marshaling: proto: Marshal called with nil")
	}
	data, err := proto.Marshal(dataMsg)
	if err != nil {
		return nil, errors.Wrap(err, "error marshaling")
	}

	paylBytes := MarshalOrPanic(
		&common.Payload{
			Header: MakePayloadHeader(payloadChannelHeader, payloadSignatureHeader),
			Data:   data,
		},
	)

	var sig []byte
	if signer != nil {
		sig, err = signer.Sign(paylBytes)
		if err != nil {
			return nil, err
		}
	}

	env := &common.Envelope{
		Payload:   paylBytes,
		Signature: sig,
	}

	return env, nil
}

// Signer is the interface needed to sign a transaction
type Signer interface {
	Sign(msg []byte) ([]byte, error)
	Serialize() ([]byte, error)
}

// CreateSignedTx assembles an Envelope message from proposal, endorsements,
// and a signer. This function should be called by a client when it has
// collected enough endorsements for a proposal to create a transaction and
// submit it to peers for ordering
func CreateSignedTx(
	proposal *peer.Proposal,
	signer Signer,
	resps ...*peer.ProposalResponse,
) (*common.Envelope, error) {
	if len(resps) == 0 {
		return nil, errors.New("at least one proposal response is required")
	}

	if signer == nil {
		return nil, errors.New("signer is required when creating a signed transaction")
	}

	// the original header
	hdr, err := UnmarshalHeader(proposal.Header)
	if err != nil {
		return nil, err
	}

	// the original payload
	pPayl, err := UnmarshalChaincodeProposalPayload(proposal.Payload)
	if err != nil {
		return nil, err
	}

	// check that the signer is the same that is referenced in the header
	signerBytes, err := signer.Serialize()
	if err != nil {
		return nil, err
	}

	shdr, err := UnmarshalSignatureHeader(hdr.SignatureHeader)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(signerBytes, shdr.Creator) {
		return nil, errors.New("signer must be the same as the one referenced in the header")
	}

	// Ensure that every response is successful. Standard Fabric transactions
	// still require one byte-identical result. A GraND proposal carrying signed
	// relaxed observations may instead form several exact groups; when one group
	// reaches the temporary 2/3 threshold, it becomes the canonical action while
	// every group is retained in the ordered GraND bundle.
	for _, r := range resps {
		if r == nil || r.Response == nil {
			return nil, errors.New("proposal response is missing")
		}
		if r.Response.Status < 200 || r.Response.Status >= 400 {
			return nil, errors.Errorf("proposal response was not successful, error code %d, msg %s", r.Response.Status, r.Response.Message)
		}
	}
	canonicalResponses, proposalGroups, err := selectGrandCanonicalResponses(resps)
	if err != nil {
		return nil, err
	}

	// fill endorsements according to their uniqueness
	endorsersUsed := make(map[string]struct{})
	var endorsements []*peer.Endorsement
	for _, r := range canonicalResponses {
		if r.Endorsement == nil {
			continue
		}
		key := string(r.Endorsement.Endorser)
		if _, used := endorsersUsed[key]; used {
			continue
		}
		endorsements = append(endorsements, r.Endorsement)
		endorsersUsed[key] = struct{}{}
	}

	if len(endorsements) == 0 {
		return nil, errors.Errorf("no endorsements")
	}

	// create ChaincodeEndorsedAction
	cea := &peer.ChaincodeEndorsedAction{ProposalResponsePayload: canonicalResponses[0].Payload, Endorsements: endorsements}

	// obtain the bytes of the proposal payload that will go to the transaction
	propPayloadBytes, err := proposalPayloadForTxWithGrandEvidence(pPayl, resps, proposalGroups)
	if err != nil {
		return nil, err
	}

	// serialize the chaincode action payload
	cap := &peer.ChaincodeActionPayload{ChaincodeProposalPayload: propPayloadBytes, Action: cea}
	capBytes, err := GetBytesChaincodeActionPayload(cap)
	if err != nil {
		return nil, err
	}

	// create a transaction
	taa := &peer.TransactionAction{Header: hdr.SignatureHeader, Payload: capBytes}
	taas := make([]*peer.TransactionAction, 1)
	taas[0] = taa
	tx := &peer.Transaction{Actions: taas}

	// serialize the tx
	txBytes, err := GetBytesTransaction(tx)
	if err != nil {
		return nil, err
	}

	// create the payload
	payl := &common.Payload{Header: hdr, Data: txBytes}
	paylBytes, err := GetBytesPayload(payl)
	if err != nil {
		return nil, err
	}

	// sign the payload
	sig, err := signer.Sign(paylBytes)
	if err != nil {
		return nil, err
	}

	// here's the envelope
	return &common.Envelope{Payload: paylBytes, Signature: sig}, nil
}

// CreateProposalResponse creates a proposal response.
func CreateProposalResponse(
	hdrbytes []byte,
	payl []byte,
	response *peer.Response,
	results []byte,
	events []byte,
	ccid *peer.ChaincodeID,
	signingEndorser Signer,
) (*peer.ProposalResponse, error) {
	hdr, err := UnmarshalHeader(hdrbytes)
	if err != nil {
		return nil, err
	}

	// obtain the proposal hash given proposal header, payload and the
	// requested visibility
	pHashBytes, err := GetProposalHash1(hdr, payl)
	if err != nil {
		return nil, errors.WithMessage(err, "error computing proposal hash")
	}

	// get the bytes of the proposal response payload - we need to sign them
	prpBytes, err := GetBytesProposalResponsePayload(pHashBytes, response, results, events, ccid)
	if err != nil {
		return nil, err
	}

	// serialize the signing identity
	endorser, err := signingEndorser.Serialize()
	if err != nil {
		return nil, errors.WithMessage(err, "error serializing signing identity")
	}

	// sign the concatenation of the proposal response and the serialized
	// endorser identity with this endorser's key
	signature, err := signingEndorser.Sign(append(prpBytes, endorser...))
	if err != nil {
		return nil, errors.WithMessage(err, "could not sign the proposal response payload")
	}

	resp := &peer.ProposalResponse{
		// Timestamp: TODO!
		Version: 1, // TODO: pick right version number
		Endorsement: &peer.Endorsement{
			Signature: signature,
			Endorser:  endorser,
		},
		Payload: prpBytes,
		Response: &peer.Response{
			Status:  200,
			Message: "OK",
		},
	}

	return resp, nil
}

// CreateProposalResponseFailure creates a proposal response for cases where
// endorsement proposal fails either due to a endorsement failure or a
// chaincode failure (chaincode response status >= shim.ERRORTHRESHOLD)
func CreateProposalResponseFailure(
	hdrbytes []byte,
	payl []byte,
	response *peer.Response,
	results []byte,
	events []byte,
	chaincodeName string,
) (*peer.ProposalResponse, error) {
	hdr, err := UnmarshalHeader(hdrbytes)
	if err != nil {
		return nil, err
	}

	// obtain the proposal hash given proposal header, payload and the requested visibility
	pHashBytes, err := GetProposalHash1(hdr, payl)
	if err != nil {
		return nil, errors.WithMessage(err, "error computing proposal hash")
	}

	// get the bytes of the proposal response payload
	prpBytes, err := GetBytesProposalResponsePayload(pHashBytes, response, results, events, &peer.ChaincodeID{Name: chaincodeName})
	if err != nil {
		return nil, err
	}

	resp := &peer.ProposalResponse{
		// Timestamp: TODO!
		Payload:  prpBytes,
		Response: response,
	}

	return resp, nil
}

// GetSignedProposal returns a signed proposal given a Proposal message and a
// signing identity
func GetSignedProposal(prop *peer.Proposal, signer Signer) (*peer.SignedProposal, error) {
	// check for nil argument
	if prop == nil || signer == nil {
		return nil, errors.New("nil arguments")
	}

	propBytes, err := proto.Marshal(prop)
	if err != nil {
		return nil, err
	}

	signature, err := signer.Sign(propBytes)
	if err != nil {
		return nil, err
	}

	return &peer.SignedProposal{ProposalBytes: propBytes, Signature: signature}, nil
}

// MockSignedEndorserProposalOrPanic creates a SignedProposal with the
// passed arguments
func MockSignedEndorserProposalOrPanic(
	channelID string,
	cs *peer.ChaincodeSpec,
	creator,
	signature []byte,
) (*peer.SignedProposal, *peer.Proposal) {
	prop, _, err := CreateChaincodeProposal(
		common.HeaderType_ENDORSER_TRANSACTION,
		channelID,
		&peer.ChaincodeInvocationSpec{ChaincodeSpec: cs},
		creator,
	)
	if err != nil {
		panic(err)
	}

	if prop == nil {
		panic(errors.New("proto: Marshal called with nil"))
	}
	propBytes, err := proto.Marshal(prop)
	if err != nil {
		panic(err)
	}

	return &peer.SignedProposal{ProposalBytes: propBytes, Signature: signature}, prop
}

func MockSignedEndorserProposal2OrPanic(
	channelID string,
	cs *peer.ChaincodeSpec,
	signer Signer,
) (*peer.SignedProposal, *peer.Proposal) {
	serializedSigner, err := signer.Serialize()
	if err != nil {
		panic(err)
	}

	prop, _, err := CreateChaincodeProposal(
		common.HeaderType_ENDORSER_TRANSACTION,
		channelID,
		&peer.ChaincodeInvocationSpec{ChaincodeSpec: &peer.ChaincodeSpec{}},
		serializedSigner,
	)
	if err != nil {
		panic(err)
	}

	sProp, err := GetSignedProposal(prop, signer)
	if err != nil {
		panic(err)
	}

	return sProp, prop
}

// GetBytesProposalPayloadForTx takes a ChaincodeProposalPayload and returns
// its serialized version according to the visibility field
func GetBytesProposalPayloadForTx(
	payload *peer.ChaincodeProposalPayload,
) ([]byte, error) {
	// check for nil argument
	if payload == nil {
		return nil, errors.New("nil arguments")
	}

	// strip the transient bytes off the payload
	cppNoTransient := &peer.ChaincodeProposalPayload{Input: payload.Input, TransientMap: nil}
	cppBytes, err := GetBytesChaincodeProposalPayload(cppNoTransient)
	if err != nil {
		return nil, err
	}

	return cppBytes, nil
}

// GetProposalHash2 gets the proposal hash - this version
// is called by the committer where the visibility policy
// has already been enforced and so we already get what
// we have to get in ccPropPayl
func GetProposalHash2(header *common.Header, ccPropPayl []byte) ([]byte, error) {
	// check for nil argument
	if header == nil ||
		header.ChannelHeader == nil ||
		header.SignatureHeader == nil ||
		ccPropPayl == nil {
		return nil, errors.New("nil arguments")
	}

	// A GraND evidence bundle is added by the client after collecting the
	// individually signed local results. It is covered by the transaction
	// creator's envelope signature, but is excluded from the common proposal
	// hash so canonical endorsers can sign the same strong/normal projection.
	cpp, unmarshalErr := UnmarshalChaincodeProposalPayload(ccPropPayl)
	if unmarshalErr != nil {
		cpp = nil
	} else {
		_, hasGrandEvidence := cpp.TransientMap[GrandRelaxedEvidenceTransientKey]
		if !hasGrandEvidence {
			cpp = nil
		}
	}
	if cpp != nil {
		delete(cpp.TransientMap, GrandRelaxedEvidenceTransientKey)
		if len(cpp.TransientMap) == 0 {
			cpp.TransientMap = nil
		}
		var err error
		ccPropPayl, err = GetBytesChaincodeProposalPayload(cpp)
		if err != nil {
			return nil, err
		}
	}

	hash := sha256.New()
	// hash the serialized Channel Header object
	hash.Write(header.ChannelHeader)
	// hash the serialized Signature Header object
	hash.Write(header.SignatureHeader)
	// hash the bytes of the chaincode proposal payload that we are given
	hash.Write(ccPropPayl)
	return hash.Sum(nil), nil
}

func proposalPayloadForTxWithGrandEvidence(
	payload *peer.ChaincodeProposalPayload,
	responses []*peer.ProposalResponse,
	proposalGroups []GrandProposalGroup,
) ([]byte, error) {
	evidence := make([][]byte, 0, len(responses))
	for _, response := range responses {
		if response == nil || response.Response == nil ||
			!strings.HasPrefix(response.Response.Message, GrandRelaxedEvidenceMessagePrefix) {
			continue
		}
		encoded := strings.TrimPrefix(response.Response.Message, GrandRelaxedEvidenceMessagePrefix)
		decoded, err := b64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, errors.Wrap(err, "decode GraND local-state endorsement")
		}
		evidence = append(evidence, decoded)
	}
	if len(evidence) == 0 {
		return GetBytesProposalPayloadForTx(payload)
	}

	sort.Slice(evidence, func(i, j int) bool {
		return bytes.Compare(evidence[i], evidence[j]) < 0
	})
	evidenceBundle := &GrandRelaxedEvidenceBundle{
		Version:        grandRelaxedEvidenceBundleVersion,
		Evidence:       evidence,
		ProposalGroups: proposalGroups,
	}
	if grandEvidenceIncludesActiveSync(evidence) {
		activeSync, err := ComputeGrandActiveSyncResult(evidence)
		if err != nil {
			return nil, errors.Wrap(err, "compute GraND active-sync result")
		}
		evidenceBundle.ActiveSync = activeSync
	}
	bundle, err := json.Marshal(evidenceBundle)
	if err != nil {
		return nil, errors.Wrap(err, "encode GraND relaxed evidence bundle")
	}
	return GetBytesChaincodeProposalPayload(&peer.ChaincodeProposalPayload{
		Input: payload.Input,
		TransientMap: map[string][]byte{
			GrandRelaxedEvidenceTransientKey: bundle,
		},
	})
}

// selectGrandCanonicalResponses preserves standard Fabric behavior unless
// every divergent response carries a GraND relaxed-state endorsement. For a
// GraND divergence, it selects an exact group only when that group contains at
// least ceil(2n/3) distinct endorsers. All exact groups are returned for
// retention in the ordered envelope.
func selectGrandCanonicalResponses(responses []*peer.ProposalResponse) ([]*peer.ProposalResponse, []GrandProposalGroup, error) {
	allMatch := true
	for index := 1; index < len(responses); index++ {
		if !bytes.Equal(responses[0].Payload, responses[index].Payload) {
			allMatch = false
			break
		}
	}
	if allMatch {
		return responses, nil, nil
	}

	type responseGroup struct {
		payload   []byte
		responses []*peer.ProposalResponse
		seen      map[string]struct{}
	}
	groupsByPayload := map[string]*responseGroup{}
	seenEndorsers := map[string]struct{}{}
	for _, response := range responses {
		if response.Endorsement == nil || len(response.Endorsement.Endorser) == 0 ||
			!strings.HasPrefix(response.Response.Message, GrandRelaxedEvidenceMessagePrefix) {
			return nil, nil, proposalResponseMismatchError(responses)
		}
		endorserKey := string(response.Endorsement.Endorser)
		if _, duplicate := seenEndorsers[endorserKey]; duplicate {
			continue
		}
		seenEndorsers[endorserKey] = struct{}{}
		payloadKey := string(response.Payload)
		group := groupsByPayload[payloadKey]
		if group == nil {
			group = &responseGroup{payload: response.Payload, seen: map[string]struct{}{}}
			groupsByPayload[payloadKey] = group
		}
		group.responses = append(group.responses, response)
		group.seen[endorserKey] = struct{}{}
	}
	if len(groupsByPayload) < 2 || len(seenEndorsers) < 2 {
		return nil, nil, proposalResponseMismatchError(responses)
	}

	groups := make([]*responseGroup, 0, len(groupsByPayload))
	for _, group := range groupsByPayload {
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool {
		if len(groups[i].responses) != len(groups[j].responses) {
			return len(groups[i].responses) > len(groups[j].responses)
		}
		return bytes.Compare(groups[i].payload, groups[j].payload) < 0
	})
	required := (2*len(seenEndorsers) + 2) / 3
	if len(groups[0].responses) < required ||
		(len(groups) > 1 && len(groups[0].responses) == len(groups[1].responses)) {
		return nil, nil, proposalResponseMismatchError(responses)
	}

	orderedGroups := make([]GrandProposalGroup, 0, len(groups))
	for _, group := range groups {
		orderedGroup := GrandProposalGroup{
			ProposalResponsePayload: append([]byte(nil), group.payload...),
			Endorsements:            make([]GrandProposalEndorsement, 0, len(group.responses)),
		}
		for _, response := range group.responses {
			orderedGroup.Endorsements = append(orderedGroup.Endorsements, GrandProposalEndorsement{
				Endorser:  append([]byte(nil), response.Endorsement.Endorser...),
				Signature: append([]byte(nil), response.Endorsement.Signature...),
			})
		}
		sort.Slice(orderedGroup.Endorsements, func(i, j int) bool {
			return bytes.Compare(orderedGroup.Endorsements[i].Endorser, orderedGroup.Endorsements[j].Endorser) < 0
		})
		orderedGroups = append(orderedGroups, orderedGroup)
	}
	return groups[0].responses, orderedGroups, nil
}

func proposalResponseMismatchError(responses []*peer.ProposalResponse) error {
	var first, different []byte
	if len(responses) > 0 && responses[0] != nil {
		first = responses[0].Payload
	}
	for _, response := range responses[1:] {
		if response != nil && !bytes.Equal(first, response.Payload) {
			different = response.Payload
			break
		}
	}
	return errors.Errorf("ProposalResponsePayloads do not match (base64): '%s' vs '%s'",
		b64.StdEncoding.EncodeToString(different), b64.StdEncoding.EncodeToString(first))
}

func grandEvidenceIncludesActiveSync(evidence [][]byte) bool {
	for _, encoded := range evidence {
		item := &grandSignedRelaxedEvidence{}
		if json.Unmarshal(encoded, item) == nil && item.Payload.Purpose == GrandActiveSyncPurpose {
			return true
		}
	}
	return false
}

// ComputeGrandActiveSyncResult deterministically selects the median quote from
// exactly three distinct peer MSP observations. Signature verification remains
// a V-stage responsibility because protoutil has no channel MSP manager.
func ComputeGrandActiveSyncResult(evidence [][]byte) (*GrandActiveSyncResult, error) {
	if len(evidence) != GrandActiveSyncObservationQuorum {
		return nil, errors.Errorf(
			"median-json-price-v1 requires exactly %d observations, got %d",
			GrandActiveSyncObservationQuorum,
			len(evidence),
		)
	}

	type observation struct {
		price int64
		value []byte
		mspID string
	}
	observations := make([]observation, 0, len(evidence))
	mspIDs := make([]string, 0, len(evidence))
	seenMSPIDs := map[string]struct{}{}
	var namespace, key, channelID, txID string
	var proposalHash, canonicalResultHash []byte

	for index, encoded := range evidence {
		item := &grandSignedRelaxedEvidence{}
		if err := json.Unmarshal(encoded, item); err != nil {
			return nil, errors.Wrapf(err, "decode active-sync observation %d", index)
		}
		payload := item.Payload
		if payload.Domain != GrandLocalEndorsementDomain || payload.Purpose != GrandActiveSyncPurpose {
			return nil, errors.Errorf("observation %d is not GraND active-sync evidence", index)
		}
		if payload.ChannelID == "" || payload.TxID == "" || len(payload.ProposalHash) == 0 || len(payload.CanonicalResultHash) == 0 {
			return nil, errors.Errorf("observation %d is missing transaction binding", index)
		}
		if len(payload.Reads) != 1 || len(payload.Writes) != 0 {
			return nil, errors.Errorf("observation %d must contain exactly one relaxed read and no writes", index)
		}
		read := payload.Reads[0]
		valueHash := sha256.Sum256(read.Value)
		if !bytes.Equal(valueHash[:], read.ValueHash) {
			return nil, errors.Errorf("observation %d has an invalid relaxed value hash", index)
		}

		identity := &mspproto.SerializedIdentity{}
		if err := proto.Unmarshal(item.Endorser, identity); err != nil {
			return nil, errors.Wrapf(err, "decode active-sync endorser %d", index)
		}
		if identity.Mspid == "" {
			return nil, errors.Errorf("observation %d has an empty MSP ID", index)
		}
		if _, duplicate := seenMSPIDs[identity.Mspid]; duplicate {
			return nil, errors.Errorf("active-sync evidence repeats MSP %s", identity.Mspid)
		}
		seenMSPIDs[identity.Mspid] = struct{}{}

		if index == 0 {
			namespace, key = read.Namespace, read.Key
			channelID, txID = payload.ChannelID, payload.TxID
			proposalHash = append([]byte(nil), payload.ProposalHash...)
			canonicalResultHash = append([]byte(nil), payload.CanonicalResultHash...)
		} else if read.Namespace != namespace || read.Key != key ||
			payload.ChannelID != channelID || payload.TxID != txID ||
			!bytes.Equal(payload.ProposalHash, proposalHash) ||
			!bytes.Equal(payload.CanonicalResultHash, canonicalResultHash) {
			return nil, errors.Errorf("observation %d is not bound to the same key and canonical transaction", index)
		}

		quote := struct {
			Price *int64 `json:"price"`
		}{}
		if err := json.Unmarshal(read.Value, &quote); err != nil || quote.Price == nil {
			return nil, errors.Errorf("observation %d is not a JSON quote with an integer price", index)
		}
		observations = append(observations, observation{
			price: *quote.Price,
			value: append([]byte(nil), read.Value...),
			mspID: identity.Mspid,
		})
		mspIDs = append(mspIDs, identity.Mspid)
	}

	sort.Slice(observations, func(i, j int) bool {
		if observations[i].price != observations[j].price {
			return observations[i].price < observations[j].price
		}
		if comparison := bytes.Compare(observations[i].value, observations[j].value); comparison != 0 {
			return comparison < 0
		}
		return observations[i].mspID < observations[j].mspID
	})
	sort.Strings(mspIDs)
	median := observations[1]
	hash := sha256.Sum256(median.value)
	return &GrandActiveSyncResult{
		Namespace: namespace,
		Key:       key,
		Value:     median.value,
		ValueHash: hash[:],
		Algorithm: GrandMedianJSONPriceAlgorithm,
		MSPIDs:    mspIDs,
	}, nil
}

// ValidateGrandActiveSyncResult recomputes the aggregate and compares every
// committed field with the client's claimed result.
func ValidateGrandActiveSyncResult(evidence [][]byte, claimed *GrandActiveSyncResult) error {
	if claimed == nil {
		return errors.New("active-sync result is missing")
	}
	expected, err := ComputeGrandActiveSyncResult(evidence)
	if err != nil {
		return err
	}
	if expected.Namespace != claimed.Namespace || expected.Key != claimed.Key ||
		expected.Algorithm != claimed.Algorithm ||
		!bytes.Equal(expected.Value, claimed.Value) ||
		!bytes.Equal(expected.ValueHash, claimed.ValueHash) ||
		len(expected.MSPIDs) != len(claimed.MSPIDs) {
		return errors.New("claimed active-sync result does not match the recomputed median")
	}
	for index := range expected.MSPIDs {
		if expected.MSPIDs[index] != claimed.MSPIDs[index] {
			return errors.New("claimed active-sync MSP set does not match the observations")
		}
	}
	return nil
}

// GetGrandRelaxedEvidenceBundleFromEnvelope extracts the full GraND bundle.
// It accepts the original array-only encoding to keep old blocks inspectable.
func GetGrandRelaxedEvidenceBundleFromEnvelope(txEnvelopeBytes []byte) (*GrandRelaxedEvidenceBundle, error) {
	bundleBytes, err := grandRelaxedEvidenceBytesFromEnvelope(txEnvelopeBytes)
	if err != nil || len(bundleBytes) == 0 {
		return nil, err
	}
	bundle := &GrandRelaxedEvidenceBundle{}
	if err := json.Unmarshal(bundleBytes, bundle); err == nil {
		if bundle.Version != grandRelaxedEvidenceBundleVersion {
			return nil, errors.Errorf("unsupported GraND relaxed evidence bundle version %d", bundle.Version)
		}
		return bundle, nil
	}
	var legacyEvidence [][]byte
	if err := json.Unmarshal(bundleBytes, &legacyEvidence); err != nil {
		return nil, errors.Wrap(err, "decode GraND relaxed evidence bundle")
	}
	return &GrandRelaxedEvidenceBundle{Version: grandRelaxedEvidenceBundleVersion, Evidence: legacyEvidence}, nil
}

// GetGrandRelaxedEvidenceFromEnvelope extracts individually signed relaxed
// evidence entries from an ordered endorser transaction.
func GetGrandRelaxedEvidenceFromEnvelope(txEnvelopeBytes []byte) ([][]byte, error) {
	bundle, err := GetGrandRelaxedEvidenceBundleFromEnvelope(txEnvelopeBytes)
	if err != nil || bundle == nil {
		return nil, err
	}
	return bundle.Evidence, nil
}

func grandRelaxedEvidenceBytesFromEnvelope(txEnvelopeBytes []byte) ([]byte, error) {
	envelope, err := GetEnvelopeFromBlock(txEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return nil, err
	}
	tx, err := UnmarshalTransaction(payload.Data)
	if err != nil {
		return nil, err
	}
	if len(tx.Actions) != 1 {
		return nil, errors.Errorf("expected one transaction action, got %d", len(tx.Actions))
	}
	action, err := UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		return nil, err
	}
	proposalPayload, err := UnmarshalChaincodeProposalPayload(action.ChaincodeProposalPayload)
	if err != nil {
		return nil, err
	}
	return proposalPayload.TransientMap[GrandRelaxedEvidenceTransientKey], nil
}

// GetGrandCanonicalEndorsersFromEnvelope returns the standard action's
// canonical endorsement identities. Validators compare these against the
// canonical proposal group; alternate groups remain in the GraND bundle.
func GetGrandCanonicalEndorsersFromEnvelope(txEnvelopeBytes []byte) ([][]byte, error) {
	envelope, err := GetEnvelopeFromBlock(txEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return nil, err
	}
	tx, err := UnmarshalTransaction(payload.Data)
	if err != nil {
		return nil, err
	}
	if len(tx.Actions) != 1 {
		return nil, errors.Errorf("expected one transaction action, got %d", len(tx.Actions))
	}
	action, err := UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		return nil, err
	}
	if action.Action == nil {
		return nil, errors.New("transaction action has no canonical endorsements")
	}
	endorsers := make([][]byte, 0, len(action.Action.Endorsements))
	for _, endorsement := range action.Action.Endorsements {
		if endorsement != nil {
			endorsers = append(endorsers, append([]byte(nil), endorsement.Endorser...))
		}
	}
	return endorsers, nil
}

// GetGrandCanonicalBindingFromEnvelope returns the proposal hash and the hash
// of the canonical ProposalResponsePayload that every local observation must
// have signed alongside its peer-specific value.
func GetGrandCanonicalBindingFromEnvelope(txEnvelopeBytes []byte) ([]byte, []byte, error) {
	envelope, err := GetEnvelopeFromBlock(txEnvelopeBytes)
	if err != nil {
		return nil, nil, err
	}
	payload, err := UnmarshalPayload(envelope.Payload)
	if err != nil {
		return nil, nil, err
	}
	tx, err := UnmarshalTransaction(payload.Data)
	if err != nil {
		return nil, nil, err
	}
	if len(tx.Actions) != 1 {
		return nil, nil, errors.Errorf("expected one transaction action, got %d", len(tx.Actions))
	}
	action, err := UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		return nil, nil, err
	}
	if action.Action == nil || len(action.Action.ProposalResponsePayload) == 0 {
		return nil, nil, errors.New("transaction action has no canonical proposal response payload")
	}
	proposalResponse, err := UnmarshalProposalResponsePayload(action.Action.ProposalResponsePayload)
	if err != nil {
		return nil, nil, err
	}
	canonicalHash := sha256.Sum256(action.Action.ProposalResponsePayload)
	return append([]byte(nil), proposalResponse.ProposalHash...), canonicalHash[:], nil
}

// GetProposalHash1 gets the proposal hash bytes after sanitizing the
// chaincode proposal payload according to the rules of visibility
func GetProposalHash1(header *common.Header, ccPropPayl []byte) ([]byte, error) {
	// check for nil argument
	if header == nil ||
		header.ChannelHeader == nil ||
		header.SignatureHeader == nil ||
		ccPropPayl == nil {
		return nil, errors.New("nil arguments")
	}

	// unmarshal the chaincode proposal payload
	cpp, err := UnmarshalChaincodeProposalPayload(ccPropPayl)
	if err != nil {
		return nil, err
	}

	ppBytes, err := GetBytesProposalPayloadForTx(cpp)
	if err != nil {
		return nil, err
	}

	hash2 := sha256.New()
	// hash the serialized Channel Header object
	hash2.Write(header.ChannelHeader)
	// hash the serialized Signature Header object
	hash2.Write(header.SignatureHeader)
	// hash of the part of the chaincode proposal payload that will go to the tx
	hash2.Write(ppBytes)
	return hash2.Sum(nil), nil
}

// GetOrComputeTxIDFromEnvelope gets the txID present in a given transaction
// envelope. If the txID is empty, it constructs the txID from nonce and
// creator fields in the envelope.
func GetOrComputeTxIDFromEnvelope(txEnvelopBytes []byte) (string, error) {
	txEnvelope, err := UnmarshalEnvelope(txEnvelopBytes)
	if err != nil {
		return "", errors.WithMessage(err, "error getting txID from envelope")
	}

	txPayload, err := UnmarshalPayload(txEnvelope.Payload)
	if err != nil {
		return "", errors.WithMessage(err, "error getting txID from payload")
	}

	if txPayload.Header == nil {
		return "", errors.New("error getting txID from header: payload header is nil")
	}

	chdr, err := UnmarshalChannelHeader(txPayload.Header.ChannelHeader)
	if err != nil {
		return "", errors.WithMessage(err, "error getting txID from channel header")
	}

	if chdr.TxId != "" {
		return chdr.TxId, nil
	}

	sighdr, err := UnmarshalSignatureHeader(txPayload.Header.SignatureHeader)
	if err != nil {
		return "", errors.WithMessage(err, "error getting nonce and creator for computing txID")
	}

	txid := ComputeTxID(sighdr.Nonce, sighdr.Creator)
	return txid, nil
}
