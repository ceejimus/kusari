package badgerstore

import (
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"
)

func getChainIDByPath(txn *badger.Txn, dirID BadgerID, path string) (BadgerID, error) {
	// the chain ID for the root path is nil slice
	currChainID := BadgerID([]byte{})
	if path == "" { // caller wants root chainID
		return currChainID, nil
	}
	// split path parts
	pathParts := splitPath(path)
	// iterate over path parts and lookup chains in path
	for _, pathPart := range pathParts {
		// get next chainID in path
		chainID, err := getChainIDFromLkp(txn, dirID, currChainID, pathPart)
		if chainID == nil || err != nil {
			return chainID, err
		}
		currChainID = chainID
	}

	return currChainID, nil
}

func addChainPathLkp(txn *badger.Txn, dirID BadgerID, path string, chainID BadgerID) error {
	// get parent dir path and node name
	dir, name := filepath.Split(path)
	// lookup dst parent chain id
	parentChainID, err := getChainIDByPath(txn, dirID, dir)
	if err != nil {
		return err
	}
	// set the value in lookup
	if err := addChainIDToLkp(txn, dirID, parentChainID, name, chainID); err != nil {
		return err
	}
	return nil
}

func moveChainPathLkp(txn *badger.Txn, dirID BadgerID, dstPath string, srcPath string) error {
	// get parent dir and name for dst
	dstDir, dstName := filepath.Split(dstPath)
	// get parent dir and name for src
	srcDir, srcName := filepath.Split(srcPath)
	// lookup dst parent chain id
	dstDirParentChainID, err := getChainIDByPath(txn, dirID, dstDir)
	if err != nil {
		return err
	}
	// lookup src parent chain id
	srcDirParentChainID, err := getChainIDByPath(txn, dirID, srcDir)
	if err != nil {
		return err
	}
	// get the src chain ID
	chainID, err := getChainIDFromLkp(txn, dirID, srcDirParentChainID, srcName)
	if chainID == nil || err != nil {
		return err
	}
	// add chain ID to dst lkp
	if err = addChainIDToLkp(txn, dirID, dstDirParentChainID, dstName, chainID); err != nil {
		return err
	}
	// delete src lkp value
	if err = removeChainIDFromLkp(txn, dirID, srcDirParentChainID, srcName); err != nil {
		return err
	}
	return nil
}

func deleteChainPathLkp(txn *badger.Txn, dirID BadgerID, path string) error {
	// get parent dir and name
	dir, name := filepath.Split(path)
	// get chain ID for parent dir
	parentChainID, err := getChainIDByPath(txn, dirID, dir)
	if err != nil {
		return err
	}
	// delete the path lkp from this chain
	if err = removeChainIDFromLkp(txn, dirID, parentChainID, name); err != nil {
		return err
	}
	return nil
}

func getChainIDFromLkp(txn *badger.Txn, dirID BadgerID, parentChainID BadgerID, name string) (BadgerID, error) {
	chainMapPrefix := makeChainPathLkpPrefix(dirID, parentChainID)
	return getID(txn, makeKey(chainMapPrefix, []byte(name)))
}

func addChainIDToLkp(txn *badger.Txn, dirID BadgerID, parentChainID BadgerID, name string, chainID BadgerID) error {
	chainMapPrefix := makeChainPathLkpPrefix(dirID, parentChainID)
	return txn.Set(makeKey(chainMapPrefix, []byte(name)), chainID.Encode())
}

func removeChainIDFromLkp(txn *badger.Txn, dirID BadgerID, parentChainID BadgerID, name string) error {
	chainMapPrefix := makeChainPathLkpPrefix(dirID, parentChainID)
	return txn.Delete(makeKey(chainMapPrefix, []byte(name)))
}

func makeChainPathLkpPrefix(dirID BadgerID, parentChainID BadgerID) []byte {
	return makeKey([]byte(LKP_CHAIN_PATH_PREFIX), dirID.Encode(), parentChainID.Encode())
}

func splitPath(path string) []string {
	if dir, name := filepath.Split(filepath.Clean(path)); dir == "" {
		return []string{name}
	} else {
		return append(splitPath(dir), name)
	}
}
