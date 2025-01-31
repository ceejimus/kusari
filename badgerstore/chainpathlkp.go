package badgerstore

import (
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"
)

func getChainIDByPath(txn *badger.Txn, dirID []byte, parentChainID []byte, path string) ([]byte, error) {
	// get parent dir path and node name
	dir, name := filepath.Split(path)
	// create lkp prefix
	chainMapPrefix := makeChainPathLkpPrefix(dirID, parentChainID)
	// get chainID for our path
	chainID, err := getValue(txn, makeKey(chainMapPrefix, []byte(name)))
	if chainID == nil || err != nil {
		return nil, err
	}
	if dir == "" {
		return chainID, nil
	}
	return getChainIDByPath(txn, dirID, chainID, name)
}

func addChainPathLkp(txn *badger.Txn, dirID []byte, path string, chainID []byte) error {
	// get parent dir path and node name
	dir, name := filepath.Split(path)
	// lookup dst parent chain id
	parentChainID, err := getChainIDByPath(txn, dirID, []byte(""), dir)
	if err != nil {
		return err
	}
	// create lkp prefix
	chainMapPrefix := makeChainPathLkpPrefix(dirID, parentChainID)
	// set the value in lookup
	if err := txn.Set(makeKey(chainMapPrefix, []byte(name)), chainID); err != nil {
		return err
	}
	return nil
}

func moveChainPathLkp(txn *badger.Txn, dirID []byte, dstPath string, srcPath string) error {
	// get parent dir and name for dst
	dstDir, dstName := filepath.Split(dstPath)
	// get parent dir and name for src
	srcDir, srcName := filepath.Split(srcPath)
	// lookup dst parent chain id
	dstDirParentChainID, err := getChainIDByPath(txn, dirID, []byte(""), dstDir)
	if err != nil {
		return err
	}
	// lookup src parent chain id
	srcDirParentChainID, err := getChainIDByPath(txn, dirID, []byte(""), srcDir)
	if err != nil {
		return err
	}
	// make lkp prefix for dst chain
	dstChainMapPrefix := makeChainPathLkpPrefix(dirID, dstDirParentChainID)
	// make lkp prefix for src chain
	srcChainMapPrefix := makeChainPathLkpPrefix(dirID, srcDirParentChainID)
	// get the src chain ID
	chainID, err := getValue(txn, makeKey(srcChainMapPrefix, []byte(srcName)))
	if chainID == nil || err != nil {
		return err
	}
	// add chain ID to dst lkp
	if err = txn.Set(makeKey(dstChainMapPrefix, []byte(dstName)), chainID); err != nil {
		return err
	}
	// delete src lkp value
	if err = txn.Delete(makeKey(srcChainMapPrefix, []byte(srcName))); err != nil {
		return err
	}
	return nil
}

func deleteChainPathLkp(txn *badger.Txn, dirID []byte, path string) error {
	// get parent dir and name
	dir, name := filepath.Split(path)
	// get chain ID for parent dir
	parentChainID, err := getChainIDByPath(txn, dirID, []byte(""), dir)
	if err != nil {
		return err
	}
	// make lkp prefix for parent chain
	chainMapPrefix := makeChainPathLkpPrefix(dirID, parentChainID)
	// delete the path lkp from this chain
	if err = txn.Delete(makeKey(chainMapPrefix, []byte(name))); err != nil {
		return err
	}
	return nil
}

func makeChainPathLkpPrefix(dirID []byte, parentChainID []byte) []byte {
	return makeKey(makeKey([]byte(LKP_CHAIN_PATH_PREFIX), dirID), parentChainID)
}

func splitPath(path string) []string {
	if dir, name := filepath.Split(filepath.Clean(path)); dir == "" {
		return []string{name}
	} else {
		return append(splitPath(dir), name)
	}
}
