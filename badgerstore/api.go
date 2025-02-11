package badgerstore

import (
	"errors"
	"fmt"

	"github.com/ceejimus/kusari/scry"
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

func (s *BadgerStore) AddDir(dir *scry.Dir) error {
	if dir.ID != nil {
		return errors.New(fmt.Sprintf("Cannot add new dir, non-nil ID %v", dir))
	}
	badgerDir := BadgerDir{Path: dir.Path}
	if err := addDir(s, &badgerDir); err != nil {
		return err
	}

	dir.ID = &badgerDir.ID
	return nil
}

func (s *BadgerStore) AddChain(chain *scry.Chain, dirID scry.ID) error {
	if chain.ID != nil {
		return errors.New(fmt.Sprintf("Cannot add new chain, non-nil ID %v", chain))
	}

	bdgID, err := toBadgerID(dirID.Encode())
	if err != nil {
		return err
	}

	badgerChain := BadgerChain{DirID: bdgID, Ino: chain.Ino}
	if err := addChain(s, &badgerChain, bdgID); err != nil {
		return err
	}

	chain.ID = &badgerChain.ID
	return nil
}

func (s *BadgerStore) AddEvent(event *scry.Event, chainID scry.ID) error {
	var id BadgerID
	if event.ID != nil {
		return errors.New(fmt.Sprintf("Cannot add new event, non-nil ID %v", event))
	}

	bdgID, err := toBadgerID(chainID.Encode())
	if err != nil {
		return err
	}

	// add new event
	badgerEvent := BadgerEvent{
		ChainID:   bdgID,
		Type:      event.Type,
		Timestamp: event.Timestamp,
		Path:      event.Path,
		ModTime:   event.ModTime,
		Size:      event.Size,
	}
	if event.Hash != nil {
		badgerEvent.Hash = *event.Hash
	}
	if err := addEvent(s, &badgerEvent, bdgID); err != nil {
		return err
	}
	// set ID
	event.ID = &id
	// set the old path
	if badgerEvent.OldPath != "" {
		event.OldPath = &badgerEvent.OldPath
	}
	return nil
}

func (s *BadgerStore) GetDirByID(dirID scry.ID) (*scry.Dir, error) {
	var dir *scry.Dir

	bdgID, err := toBadgerID(dirID.Encode())
	if err != nil {
		return nil, err
	}

	if err := s.db.View(func(txn *badger.Txn) error {
		bdgDir, err := getDirByID(txn, bdgID)
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

func (s *BadgerStore) GetDirByPath(path string) (*scry.Dir, error) {
	var dir *scry.Dir
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

func (s *BadgerStore) GetChainByID(chainID scry.ID) (*scry.Chain, error) {
	var chain *scry.Chain
	bdgID, err := toBadgerID(chainID.Encode())
	if err != nil {
		return nil, err
	}
	if err := s.db.View(func(txn *badger.Txn) error {
		bdgChain, err := getChainByID(txn, bdgID)
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

func (s *BadgerStore) GetChainByPath(dirID scry.ID, path string) (*scry.Chain, error) {
	var chain *scry.Chain
	bdgID, err := toBadgerID(dirID.Encode())
	if err != nil {
		return nil, err
	}
	if err := s.db.View(func(txn *badger.Txn) error {
		chainID, err := getChainIDByPath(txn, bdgID, path)
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

func (s *BadgerStore) GetChainByIno(ino uint64) (*scry.Chain, error) {
	var chain *scry.Chain
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

func (s *BadgerStore) GetEventByID(eventID scry.ID) (*scry.Event, error) {
	panic("GetEventByID not implemented")
}

func (s *BadgerStore) GetPrevEvent(eventID scry.ID) (*scry.Event, error) {
	panic("GetPrevEvent not implemented")
}

func (s *BadgerStore) GetNextEvent(eventID scry.ID) (*scry.Event, error) {
	panic("GetNextEvent not implemented")
}

func (s *BadgerStore) GetDirs() ([]scry.Dir, error) {
	var bdgDirs []BadgerDir
	if err := s.db.View(func(txn *badger.Txn) error {
		var err error
		prefix := []byte(PFX_DIR + ":")
		bdgDirs, err = iterObjects[BadgerDir](txn, prefix)
		return err
	}); err != nil {
		return nil, err
	}

	dirs := make([]scry.Dir, len(bdgDirs))
	for i, bdgDir := range bdgDirs {
		dirs[i] = badgerDirToDir(bdgDir)
	}

	return dirs, nil
}

func (s *BadgerStore) GetChainsInDir(dirID scry.ID) ([]scry.Chain, error) {
	var bdgChains []BadgerChain
	if err := s.db.View(func(txn *badger.Txn) error {
		var ids [][]byte
		var err error

		bdgID, err := toBadgerID(dirID.Encode())
		if err != nil {
			return err
		}

		prefix := append(makeKey([]byte(LKP_CHAIN_DIR), bdgID.Encode()), []byte(":")...)
		ids, err = iterVals(txn, prefix)
		if err != nil {
			return err
		}
		bdgChains = make([]BadgerChain, len(ids))
		for i, id := range ids {
			chainID, err := toBadgerID(id)
			if err != nil {
				return err
			}
			bdgChain, err := getChainByID(txn, chainID)
			if err != nil {
				return err
			}
			bdgChains[i] = *bdgChain
		}
		return nil
	}); err != nil {
		return nil, err
	}

	chains := make([]scry.Chain, len(bdgChains))
	for i, bdgChain := range bdgChains {
		chains[i] = badgerChainToChain(bdgChain)
	}

	return chains, nil
}

func (s *BadgerStore) GetEventsInChain(chainID scry.ID) ([]scry.Event, error) {
	var bdgEvents []BadgerEvent
	bdgID, err := toBadgerID(chainID.Encode())
	if err != nil {
		return nil, err
	}

	if err := s.db.View(func(txn *badger.Txn) error {
		bdgEvent, err := getChainHead(txn, bdgID)
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

	events := make([]scry.Event, len(bdgEvents))
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

func badgerDirToDir(bdgDir BadgerDir) scry.Dir {
	return scry.Dir{
		ID:   &bdgDir.ID,
		Path: bdgDir.Path,
	}
}

func badgerChainToChain(bdgChain BadgerChain) scry.Chain {
	id := new(BadgerID)
	*id = bdgChain.ID
	return scry.Chain{
		ID:  id,
		Ino: bdgChain.Ino,
	}
}

func badgerEventToEvent(bdgEvent BadgerEvent) scry.Event {
	var hash *string
	var oldPath *string
	if bdgEvent.Hash != "" {
		hash = &bdgEvent.Hash
	}
	if bdgEvent.OldPath != "" {
		oldPath = &bdgEvent.OldPath
	}
	id := *&bdgEvent.ID
	return scry.Event{
		ID:        &id,
		Timestamp: bdgEvent.Timestamp,
		Path:      bdgEvent.Path,
		OldPath:   oldPath,
		Type:      bdgEvent.Type,
		Size:      bdgEvent.Size,
		Hash:      hash,
		ModTime:   bdgEvent.ModTime,
	}
}
