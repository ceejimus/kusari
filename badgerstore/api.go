package badgerstore

import (
	"errors"
	"fmt"

	"atmoscape.net/fileserver/syncd"
	badger "github.com/dgraph-io/badger/v4"
)

func NewBadgerStore(dbDir string) (*BadgerStore, error) {
	db, err := badger.Open(badger.DefaultOptions(dbDir))
	if err != nil {
		return nil, err
	}
	seqMap := make(map[string]*badger.Sequence, len(SEQ_KEYS))
	for _, seqKey := range SEQ_KEYS {
		seq, err := db.GetSequence([]byte(seqKey), 1000)
		if err != nil {
			return nil, err
		}
		seqMap[seqKey] = seq
	}
	return &BadgerStore{
		db:     db,
		seqMap: seqMap,
	}, nil
}

func (s *BadgerStore) AddDir(dir *syncd.Dir) error {
	if dir.ID != nil {
		return errors.New(fmt.Sprintf("Cannot add new dir, non-nil ID %v", dir))
	}
	badgerDir := BadgerDir{Path: dir.Path}
	if err := addDir(s, &badgerDir); err != nil {
		return err
	}

	dir.ID = badgerDir.ID
	return nil
}

func (s *BadgerStore) AddChain(chain *syncd.Chain, dirID []byte) error {
	if chain.ID != nil {
		return errors.New(fmt.Sprintf("Cannot add new chain, non-nil ID %v", chain))
	}

	badgerChain := BadgerChain{DirID: dirID, Ino: chain.Ino}
	if err := addChain(s, &badgerChain, dirID); err != nil {
		return err
	}

	chain.ID = badgerChain.ID
	return nil
}

func (s *BadgerStore) AddEvent(event *syncd.Event, chainID []byte) error {
	var id []byte
	if event.ID != nil {
		return errors.New(fmt.Sprintf("Cannot add new event, non-nil ID %v", event))
	}
	// add new event
	badgerEvent := BadgerEvent{
		ChainID:   chainID,
		Type:      event.Type,
		Timestamp: event.Timestamp,
		Path:      event.Path,
		ModTime:   event.ModTime,
		Size:      event.Size,
	}
	if event.Hash != nil {
		badgerEvent.Hash = *event.Hash
	}
	if err := addEvent(s, &badgerEvent, chainID); err != nil {
		return err
	}
	// set ID
	event.ID = id
	return nil
}

func (s *BadgerStore) GetDirByID(id []byte) (*syncd.Dir, error) {
	var dir *syncd.Dir
	if err := s.db.View(func(txn *badger.Txn) error {
		bdgDir, err := getDirByID(txn, id)
		if bdgDir == nil || err != nil {
			return err
		}
		converted := badgerDirToDir(*bdgDir)
		dir = &converted
		return nil
	}); err != nil {
		return nil, err
	}
	return dir, nil
}

func (s *BadgerStore) GetDirByPath(path string) (*syncd.Dir, error) {
	var dir *syncd.Dir
	if err := s.db.View(func(txn *badger.Txn) error {
		bdgDir, err := getDirByPath(txn, path)
		if bdgDir == nil || err != nil {
			return err
		}
		converted := badgerDirToDir(*bdgDir)
		dir = &converted
		return nil
	}); err != nil {
		return nil, err
	}
	return dir, nil
}

func (s *BadgerStore) GetChainByID(id []byte) (*syncd.Chain, error) {
	var chain *syncd.Chain
	if err := s.db.View(func(txn *badger.Txn) error {
		bdgChain, err := getChainByID(txn, id)
		if err != nil {
			return err
		}
		converted := badgerChainToChain(*bdgChain)
		chain = &converted
		return nil
	}); err != nil {
		return nil, err
	}
	return chain, nil
}

func (s *BadgerStore) GetChainByPath(dirID []byte, path string) (*syncd.Chain, error) {
	var chain *syncd.Chain
	if err := s.db.View(func(txn *badger.Txn) error {
		chainID, err := getChainIDByPath(txn, dirID, path)
		if err != nil {
			return nil
		}
		bdgChain, err := getChainByID(txn, chainID)
		if err != nil {
			return err
		}
		converted := badgerChainToChain(*bdgChain)
		chain = &converted
		return nil
	}); err != nil {
		return nil, err
	}
	return chain, nil
}

func (s *BadgerStore) GetChainByIno(ino uint64) (*syncd.Chain, error) {
	var chain *syncd.Chain
	if err := s.db.View(func(txn *badger.Txn) error {
		bdgChain, err := getChainByIno(txn, ino)
		if bdgChain == nil || err != nil {
			return err
		}
		converted := badgerChainToChain(*bdgChain)
		chain = &converted
		return nil
	}); err != nil {
		return nil, err
	}
	return chain, nil
}

func (s *BadgerStore) GetEventByID(id []byte) (*syncd.Event, error) {
	panic("GetEventByID not implemented")
}

func (s *BadgerStore) GetDirs() ([]syncd.Dir, error) {
	var bdgDirs []BadgerDir
	if err := s.db.View(func(txn *badger.Txn) error {
		var err error
		prefix := []byte(PFX_DIR + ":")
		bdgDirs, err = iterObjects[BadgerDir](txn, prefix)
		return err
	}); err != nil {
		return nil, err
	}

	dirs := make([]syncd.Dir, len(bdgDirs))
	for i, bdgDir := range bdgDirs {
		dirs[i] = badgerDirToDir(bdgDir)
	}

	return dirs, nil
}

func (s *BadgerStore) GetChainsInDir(id []byte) ([]syncd.Chain, error) {
	var bdgChains []BadgerChain
	if err := s.db.View(func(txn *badger.Txn) error {
		var ids [][]byte
		var err error
		prefix := append(makeKey([]byte(LKP_CHAIN_DIR), id), []byte(":")...)
		ids, err = iterVals(txn, prefix)
		if err != nil {
			return err
		}
		bdgChains = make([]BadgerChain, len(ids))
		for i, id := range ids {
			bdgChain, err := getChainByID(txn, id)
			if err != nil {
				return err
			}
			bdgChains[i] = *bdgChain
		}
		return nil
	}); err != nil {
		return nil, err
	}

	chains := make([]syncd.Chain, len(bdgChains))
	for i, bdgChain := range bdgChains {
		chains[i] = badgerChainToChain(bdgChain)
	}

	return chains, nil
}

func (s *BadgerStore) GetEventsInChain(chainID []byte) ([]syncd.Event, error) {
	var bdgEvents []BadgerEvent
	if err := s.db.View(func(txn *badger.Txn) error {
		bdgEvent, err := getChainHead(txn, chainID)
		if err != nil {
			return err
		}
		for {
			if bdgEvent == nil {
				break
			}
			bdgEvents = append(bdgEvents, *bdgEvent)
			bdgEvent, err = getEventNext(txn, bdgEvent.ID)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	events := make([]syncd.Event, len(bdgEvents))
	for i, bdgEvent := range bdgEvents {
		events[i] = badgerEventToEvent(bdgEvent)
	}

	return events, nil
}

func (s *BadgerStore) Close() error {
	for _, seq := range s.seqMap {
		seq.Release()
	}
	return s.db.Close()
}

func badgerDirToDir(bdgDir BadgerDir) syncd.Dir {
	return syncd.Dir{
		ID:   bdgDir.ID,
		Path: bdgDir.Path,
	}
}

func badgerChainToChain(bdgChain BadgerChain) syncd.Chain {
	return syncd.Chain{
		ID:  bdgChain.ID,
		Ino: bdgChain.Ino,
	}
}

func badgerEventToEvent(bdgEvent BadgerEvent) syncd.Event {
	var hash *string
	if bdgEvent.Hash != "" {
		hash = &bdgEvent.Hash
	}
	return syncd.Event{
		ID:        bdgEvent.ID,
		Timestamp: bdgEvent.Timestamp,
		Path:      bdgEvent.Path,
		Type:      bdgEvent.Type,
		Size:      bdgEvent.Size,
		Hash:      hash,
		ModTime:   bdgEvent.ModTime,
	}
}
