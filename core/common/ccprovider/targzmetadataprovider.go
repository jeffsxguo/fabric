/*
# Copyright State Street Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
*/
package ccprovider

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	stateconsistency "github.com/hyperledger/fabric/core/ledger/consistency"
)

// The targz metadata provider is reference for other providers (such as what CAR would
// implement). GraND additionally carries its offline consistency result through
// the same lifecycle metadata path.
const (
	ccPackageStatedbDir = "META-INF/statedb/"
	ccPackageGraNDDir   = "META-INF/grand/"
)

type PersistenceAdapter func([]byte) ([]byte, error)

func (pa PersistenceAdapter) GetDBArtifacts(codePackage []byte) ([]byte, error) {
	return pa(codePackage)
}

// MetadataAsTarEntries extracts deploy-time metadata from a chaincode package.
func MetadataAsTarEntries(code []byte) ([]byte, error) {
	is := bytes.NewReader(code)
	gr, err := gzip.NewReader(is)
	if err != nil {
		ccproviderLogger.Errorf("Failure opening codepackage gzip stream: %s", err)
		return nil, err
	}

	statedbTarBuffer := bytes.NewBuffer(nil)
	tw := tar.NewWriter(statedbTarBuffer)

	tr := tar.NewReader(gr)
	grandProgramFound := false

	// For each file in the code package tar, add state DB artifacts and GraND's
	// offline analysis result to the lifecycle artifact tar.
	for {
		header, err := tr.Next()
		if err == io.EOF {
			// We only get here if there are no more entries to scan
			break
		}
		if err != nil {
			return nil, err
		}

		if !strings.HasPrefix(header.Name, ccPackageStatedbDir) &&
			!strings.HasPrefix(header.Name, ccPackageGraNDDir) {
			continue
		}
		if strings.HasPrefix(header.Name, ccPackageGraNDDir) &&
			header.Name != stateconsistency.ContractProgramArtifact {
			return nil, fmt.Errorf(
				"unsupported GraND chaincode metadata path %q: expected %q",
				header.Name,
				stateconsistency.ContractProgramArtifact,
			)
		}
		if header.Name == stateconsistency.ContractProgramArtifact {
			if grandProgramFound {
				return nil, fmt.Errorf("duplicate %s in chaincode package", header.Name)
			}
			grandProgramFound = true
			contents, err := io.ReadAll(tr)
			if err != nil {
				return nil, err
			}
			if _, err := stateconsistency.ParseContractProgram(contents); err != nil {
				return nil, err
			}
			header.Size = int64(len(contents))
			if err = tw.WriteHeader(header); err != nil {
				return nil, err
			}
			if _, err = tw.Write(contents); err != nil {
				return nil, err
			}
			continue
		}

		if err = tw.WriteHeader(header); err != nil {
			ccproviderLogger.Error("Error adding header to statedb tar:", err, header.Name)
			return nil, err
		}
		if _, err := io.Copy(tw, tr); err != nil {
			ccproviderLogger.Error("Error copying file to statedb tar:", err, header.Name)
			return nil, err
		}
		ccproviderLogger.Debug("Wrote file to statedb tar:", header.Name)
	}

	if err = tw.Close(); err != nil {
		return nil, err
	}

	ccproviderLogger.Debug("Created metadata tar")

	return statedbTarBuffer.Bytes(), nil
}
