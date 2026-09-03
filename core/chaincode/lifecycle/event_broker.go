/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lifecycle

import (
	"archive/tar"
	"bytes"
	"io"
	"sync"

	"github.com/hyperledger/fabric/core/container/externalbuilder"
	"github.com/hyperledger/fabric/core/ledger"
	stateconsistency "github.com/hyperledger/fabric/core/ledger/consistency"
	"github.com/pkg/errors"
)

// EventBroker receives events from lifecycle cache and in turn invokes the registered listeners
type EventBroker struct {
	chaincodeStore       ChaincodeStore
	ebMetadata           *externalbuilder.MetadataProvider
	pkgParser            PackageParser
	defineCallbackStatus *sync.Map

	mutex     sync.Mutex
	listeners map[string][]ledger.ChaincodeLifecycleEventListener
}

func NewEventBroker(chaincodeStore ChaincodeStore, pkgParser PackageParser, ebMetadata *externalbuilder.MetadataProvider) *EventBroker {
	return &EventBroker{
		chaincodeStore:       chaincodeStore,
		ebMetadata:           ebMetadata,
		pkgParser:            pkgParser,
		listeners:            make(map[string][]ledger.ChaincodeLifecycleEventListener),
		defineCallbackStatus: &sync.Map{},
	}
}

func (b *EventBroker) RegisterListener(
	channelID string,
	listener ledger.ChaincodeLifecycleEventListener,
	existingCachedChaincodes map[string]*CachedChaincodeDefinition,
) {
	// when invoking chaincode event listener with existing invocable chaincodes, we logs
	// errors instead of returning the error from this function to keep the consustent behavior
	// similar to the code path when we invoke the listener later on as a response to the chaincode
	// lifecycle events. See other functions below for details on this behavior.
	for chaincodeName, cachedChaincode := range existingCachedChaincodes {
		if !isChaincodeInvocable(cachedChaincode) {
			continue
		}

		dbArtifacts, err := b.loadDBArtifacts(cachedChaincode.InstallInfo.PackageID)
		if err != nil {
			logger.Errorw(
				"error while loading db artifacts for chaincode package. Continuing...",
				"packageID", cachedChaincode.InstallInfo.PackageID,
				"error", err,
			)
			continue
		}
		legacyDefinition := &ledger.ChaincodeDefinition{
			Name:              chaincodeName,
			Version:           cachedChaincode.Definition.EndorsementInfo.Version,
			Hash:              []byte(cachedChaincode.InstallInfo.PackageID),
			CollectionConfigs: cachedChaincode.Definition.Collections,
		}

		if err := listener.HandleChaincodeDeploy(legacyDefinition, dbArtifacts); err != nil {
			logger.Errorw(
				"error while invoking chaincode lifecycle events listener. Continuing...",
				"packageID", cachedChaincode.InstallInfo.PackageID,
				"error", err,
			)
		}
		listener.ChaincodeDeployDone(true)
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.listeners[channelID] = append(b.listeners[channelID], listener)
}

// ProcessInstallEvent gets invoked when a chaincode is installed
func (b *EventBroker) ProcessInstallEvent(localChaincode *LocalChaincode) {
	logger.Debugf("ProcessInstallEvent() - localChaincode = %s", localChaincode.Info)
	dbArtifacts, err := b.loadDBArtifacts(localChaincode.Info.PackageID)
	if err != nil {
		logger.Errorf("Error while loading db artifacts for chaincode package with package ID [%s]: %s",
			localChaincode.Info.PackageID, err)
		return
	}
	for channelID, channelCache := range localChaincode.References {
		listenersInvokedOnChannel := false
		for chaincodeName, cachedChaincode := range channelCache {
			if !isChaincodeInvocable(cachedChaincode) {
				continue
			}
			ccdef := &ledger.ChaincodeDefinition{
				Name:              chaincodeName,
				Version:           cachedChaincode.Definition.EndorsementInfo.Version,
				Hash:              []byte(cachedChaincode.InstallInfo.PackageID),
				CollectionConfigs: cachedChaincode.Definition.Collections,
			}
			b.invokeListeners(channelID, ccdef, dbArtifacts)
			listenersInvokedOnChannel = true
		}
		if listenersInvokedOnChannel {
			// In the legacy lscc the install was split into two phases
			// In the first phase, all the listener will be invoked and in the second phase,
			// the install will proceed and finally will, give a call back whether the install
			// is succeeded.
			// The purpose of splitting this in two phases was to essentially not miss on an install
			// event in the case of a system crash immediately after install and before the listeners
			// gets a chance.
			// However, in the current install model, the lifecycle cache receives the event only after
			// the install is complete. So, for now, call the done on the listeners with a hard-wired 'true'
			b.invokeDoneOnListeners(channelID, true)
		}
	}
}

// ProcessApproveOrDefineEvent gets invoked by an event that makes approve and define to be true
// This should be OK even if this function gets invoked on defined and approved events separately because
// the first check in this function evaluates the final condition. However, the current cache implementation
// invokes this function when approve and define both become true.
func (b *EventBroker) ProcessApproveOrDefineEvent(channelID string, chaincodeName string, cachedChaincode *CachedChaincodeDefinition) {
	logger.Debugw("processApproveOrDefineEvent()", "channelID", channelID, "chaincodeName", chaincodeName, "cachedChaincode", cachedChaincode)
	if !isChaincodeInvocable(cachedChaincode) {
		return
	}
	dbArtifacts, err := b.loadDBArtifacts(cachedChaincode.InstallInfo.PackageID)
	if err != nil {
		logger.Errorf("Error while loading db artifacts for chaincode package with package ID [%s]: %s",
			cachedChaincode.InstallInfo.PackageID, err)
		return
	}
	ccdef := &ledger.ChaincodeDefinition{
		Name:              chaincodeName,
		Version:           cachedChaincode.Definition.EndorsementInfo.Version,
		Hash:              []byte(cachedChaincode.InstallInfo.PackageID),
		CollectionConfigs: cachedChaincode.Definition.Collections,
	}
	b.invokeListeners(channelID, ccdef, dbArtifacts)
	b.defineCallbackStatus.Store(channelID, struct{}{})
}

// ApproveOrDefineCommitted gets invoked after the commit of state updates that triggered the invocation of
// "ProcessApproveOrDefineEvent" function
func (b *EventBroker) ApproveOrDefineCommitted(channelID string) {
	_, ok := b.defineCallbackStatus.Load(channelID)
	if !ok {
		return
	}
	b.invokeDoneOnListeners(channelID, true)
	b.defineCallbackStatus.Delete(channelID)
}

func (b *EventBroker) invokeListeners(channelID string, legacyDefinition *ledger.ChaincodeDefinition, dbArtifacts []byte) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	channelListeners := b.listeners[channelID]
	for _, l := range channelListeners {
		if err := l.HandleChaincodeDeploy(legacyDefinition, dbArtifacts); err != nil {
			// If a listener return this error and we propagate this error up the stack,
			// following are the implications:
			//
			// 1) If this path gets called from the chaincode install operation, the install operation will need to
			// handle the error, perhaps by aborting the install operation
			// 2) If this path gets called from the block commit (that includes chaincode approve/define transaction)
			// it will result in a peer panic.
			//
			// The behavior mentioned in (2) i.e., the installation of malformed chaincode package resulting in a
			// peer panic on approve/define transaction commit may not be a desired behavior.
			// Primarily because, a) the installation of chaincode is not a fundamental requirement for committer to function
			// and b) typically, it may take longer dev cycles to fix the chaincode package issues as opposed to some admin
			// operation (say, restart couchdb). Note that chaincode uninstall is not currently supported.
			//
			// In addition, another implication is that the behavior will be inconsistent on different peers. In the case of
			// a faulty package, some peers may fail on install while others will report a success in installation and fail
			// later at the approve/define commit time.
			//
			// So, instead of throwing this error up logging this here.
			logger.Errorf("Error from listener during processing chaincode lifecycle event - %+v", errors.WithStack(err))
		}
	}
}

func (b *EventBroker) invokeDoneOnListeners(channelID string, succeeded bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	channelListeners := b.listeners[channelID]
	for _, l := range channelListeners {
		l.ChaincodeDeployDone(succeeded)
	}
}

func (b *EventBroker) loadDBArtifacts(packageID string) ([]byte, error) {
	md, err := b.ebMetadata.PackageMetadata(packageID)
	if err != nil {
		return nil, err
	}

	pkgBytes, err := b.chaincodeStore.Load(packageID)
	if err != nil {
		return nil, err
	}
	pkg, err := b.pkgParser.Parse(pkgBytes)
	if err != nil {
		return nil, err
	}
	if md != nil {
		return mergePackageConsistencyProgram(md, pkg.DBArtifacts)
	}
	return pkg.DBArtifacts, nil
}

// mergePackageConsistencyProgram preserves external-builder release metadata
// while restoring the consistency program bound into the installed package.
// Fabric's external-builder metadata takes precedence over ordinary package
// indexes, but it must not hide or replace GraND's package-ID-bound analysis.
func mergePackageConsistencyProgram(externalArtifacts, packageArtifacts []byte) ([]byte, error) {
	_, artifact, found, err := stateconsistency.ExtractContractProgram(packageArtifacts)
	if err != nil {
		return nil, err
	}
	if !found {
		return externalArtifacts, nil
	}

	result := bytes.NewBuffer(nil)
	writer := tar.NewWriter(result)
	reader := tar.NewReader(bytes.NewReader(externalArtifacts))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "read external-builder deployment metadata")
		}
		if header.Name == stateconsistency.ContractProgramArtifact {
			// The installed package is the source of truth because its hash is
			// included in the lifecycle package ID.
			continue
		}
		if err := writer.WriteHeader(header); err != nil {
			return nil, errors.Wrap(err, "copy external-builder deployment metadata header")
		}
		if _, err := io.Copy(writer, reader); err != nil {
			return nil, errors.Wrap(err, "copy external-builder deployment metadata entry")
		}
	}
	if err := writer.WriteHeader(&tar.Header{
		Name: stateconsistency.ContractProgramArtifact,
		Mode: 0o644,
		Size: int64(len(artifact)),
	}); err != nil {
		return nil, errors.Wrap(err, "write GraND consistency deployment metadata header")
	}
	if _, err := writer.Write(artifact); err != nil {
		return nil, errors.Wrap(err, "write GraND consistency deployment metadata")
	}
	if err := writer.Close(); err != nil {
		return nil, errors.Wrap(err, "close merged deployment metadata")
	}
	return result.Bytes(), nil
}

// isChaincodeInvocable returns true iff a chaincode is approved and installed and defined
func isChaincodeInvocable(ccInfo *CachedChaincodeDefinition) bool {
	return ccInfo.Approved && ccInfo.InstallInfo != nil && ccInfo.Definition != nil
}
