package badgerstore

import (
	"errors"
	"fmt"
	"time"

	"atmoscape.net/fileserver/logger"
	"atmoscape.net/fileserver/syncd"
	badger "github.com/dgraph-io/badger/v4"
)

const PFX_DIR = "dir"
const LKP_DIR_PATH = "lkp:dir:path"

const PFX_CHAIN = "chain"
const LKP_CHAIN_INO = "lkp:chain:ino"
const LKP_CHAIN_DIR = "lkp:chain:dir"

const PFX_EVENT = "event"
const LKP_CHAIN_HEAD = "lkp:event:head"
const LKP_CHAIN_TAIL = "lkp:event:tail"
const LKP_EVENT_NEXT = "lkp:event:next"
const LKP_CHAIN_PATH_PREFIX = "lkp:chain:path"

var SEQ_KEYS = []string{PFX_DIR, PFX_CHAIN, PFX_EVENT, LKP_CHAIN_DIR}

// type BadgerObject interface {
// 	GetID() []byte
// 	SetID([]byte)
// }

type BadgerDir struct {
	Path string // relative to configured top-level directory
}

type BadgerChain struct {
	DirID []byte // the dir ID to which this chain belongs
	Ino   uint64 // the ino of the syncd inode
}

type BadgerEvent struct {
	ChainID   []byte          // the chain ID to which this event belongs
	Type      syncd.EventType // "create", "modify", "delete", "rename", etc.
	Timestamp time.Time       // time event processed
	Path      string          // relative path of file
	ModTime   time.Time       // modification time
	Hash      string          // file hash (if file)
	Size      uint64          // file size
}

type SeqMap map[string]*badger.Sequence

type BadgerStore struct {
	db     *badger.DB
	seqMap SeqMap
}

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

func (s *BadgerStore) nextIDFor(prefix string) ([]byte, error) {
	seq, ok := s.seqMap[prefix]
	if !ok {
		newSeq, err := s.db.GetSequence([]byte(prefix), 1000)
		if err != nil {
			return nil, err
		}
		s.seqMap[prefix] = newSeq
		seq = newSeq
	}
	idInt, err := seq.Next()
	if err != nil {
		return nil, err
	}
	return uint64ToBytes(idInt), nil
}

func (s *BadgerStore) AddDir(dir syncd.Dir) (*syncd.Dir, error) {
	var id []byte
	if dir.ID != nil {
		return nil, errors.New(fmt.Sprintf("Cannot add new dir, non-nil ID %v", dir))
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		existingDir, err := getDirByPath(txn, dir.Path)
		if err != nil {
			return err
		}
		if existingDir != nil {
			return errors.New(fmt.Sprintf("Cannot add Dir, existing dir w/ path %s", dir.Path))
		}

		if id, err = s.nextIDFor(PFX_DIR); err != nil {
			return err
		}

		badgerDir := BadgerDir{Path: dir.Path}
		if err = addObject(txn, makeKey([]byte(PFX_DIR), id), badgerDir); err != nil {
			return err
		}

		// add path to dir lkp
		if err = txn.Set(makeKey([]byte(LKP_DIR_PATH), []byte(dir.Path)), id); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	dir.ID = id
	return &dir, nil
}

func (s *BadgerStore) AddChain(chain syncd.Chain, dirID []byte) (*syncd.Chain, error) {
	// const PFX_CHAIN = "chain"
	// const LKP_CHAIN_INO = "lkp:chain:ino"
	// const LKP_CHAIN_DIR = "lkp:chain:dir"
	var id []byte
	if chain.ID != nil {
		return nil, errors.New(fmt.Sprintf("Cannot add new chain, non-nil ID %v", chain))
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		// check dir exists
		dir, err := getDirByID(txn, dirID)
		if dir == nil || err != nil {
			return errors.New(fmt.Sprintf("Cannot add new chain, nonexistent dir w/ id: %v", dirID))
		}
		// NOTE: at this point we might to an ino lkp check, however it's possible for inodes to get reused
		// this would start a new chain, we just need to be aware of this
		// add new chain
		if id, err = s.nextIDFor(PFX_CHAIN); err != nil {
			return err
		}
		badgerChain := BadgerChain{DirID: dirID, Ino: chain.Ino}
		if err = addObject(txn, makeKey([]byte(PFX_CHAIN), id), badgerChain); err != nil {
			return err
		}
		// add chain to ino lkp
		if err = txn.Set(makeKey([]byte(LKP_CHAIN_INO), uint64ToBytes(chain.Ino)), id); err != nil {
			return err
		}
		// add chain to dir lkp
		lkpID, err := s.nextIDFor(LKP_CHAIN_DIR)
		if err != nil {
			return err
		}
		if err = txn.Set(makeKey([]byte(LKP_CHAIN_DIR), dirID, lkpID), id); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	chain.ID = id
	return &chain, nil
}

func (s *BadgerStore) AddEvent(event syncd.Event, chainID []byte) (*syncd.Event, error) {
	// const PFX_EVENT = "event"
	// const LKP_CHAIN_PATH_PREFIX = "lkp:chain:path"
	// const LKP_CHAIN_HEAD = "lkp:event:head"
	// const LKP_CHAIN_TAIL = "lkp:event:tail"
	// const LKP_EVENT_NEXT = "lkp:event:next"
	var id []byte
	if event.ID != nil {
		return nil, errors.New(fmt.Sprintf("Cannot add new event, non-nil ID %v", event))
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		// check chain exists
		chain, err := getChainByID(txn, chainID)
		if chain == nil || err != nil {
			return errors.New(fmt.Sprintf("Cannot add new event, nonexistent chain w/ id: %v", chainID))
		}
		if id, err = s.nextIDFor(PFX_EVENT); err != nil {
			return err
		}
		hash := ""
		if event.Hash != nil {
			hash = *event.Hash
		}
		// add new event
		badgerEvent := BadgerEvent{
			ChainID:   chainID,
			Type:      event.Type,
			Timestamp: event.Timestamp,
			Path:      event.Path,
			ModTime:   event.ModTime,
			Hash:      hash,
			Size:      event.Size,
		}
		if err = addObject(txn, makeKey([]byte(PFX_EVENT), id), badgerEvent); err != nil {
			return err
		}
		// link events to and w/in chain
		headID, err := getValue(txn, makeKey([]byte(LKP_CHAIN_HEAD), chainID))
		if err != nil {
			return nil
		}
		tailID, err := getValue(txn, makeKey([]byte(LKP_CHAIN_HEAD), chainID))
		if err != nil {
			return nil
		}
		if headID == nil { // this is the first event in chain
			if err = txn.Set(makeKey([]byte(LKP_CHAIN_HEAD), chainID), id); err != nil {
				return err
			}
		} else {
			if err = txn.Set(makeKey([]byte(LKP_EVENT_NEXT), tailID), id); err != nil {
				return err
			}
			// update the path lookup depending on event type
			// if err = updateChainLkps(txn, chainID, tailID)

			// set the tail
			if err = txn.Set(makeKey([]byte(LKP_CHAIN_TAIL), chainID), id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	event.ID = id
	return &event, nil
}

func updateChainLkps(txn *badger.Txn, chainID []byte, event *BadgerEvent, tailID []byte) error {
	var tail *BadgerEvent
	chain, err := getObject[BadgerChain](txn, makeKey([]byte(PFX_CHAIN), chainID))
	if err != nil {
		return err
	}
	if tailID != nil {
		tail, err := getObject[BadgerEvent](txn, makeKey([]byte(PFX_EVENT), tailID))
		if err != nil {
			return err
		}
		if tail == nil {
			return errors.New(fmt.Sprintf("Failed to update chain links, non existent tail w/ ID: %s", tailID))
		}
	}
	if event.Type == syncd.Create && tail != nil && tail.Type == syncd.Rename {
		if err := moveChainPathLkp(txn, chain.DirID, event.Path, tail.Path); err != nil {
			return err
		}
	} else if event.Type == syncd.Remove {
		if err := deleteChainPathLkp(txn, chain.DirID, event.Path); err != nil {
			return err
		}
		if err := txn.Delete(makeKey([]byte(LKP_CHAIN_INO), uint64ToBytes(chain.Ino))); err != nil {
			return err
		}
	} else {
		chainID, err := getChainIDByPath(txn, chain.DirID, []byte(""), event.Path)
		if err != nil {
			return err
		}
		if chainID == nil {
			if err := addChainPathLkp(txn, chain.DirID, event.Path, chainID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *BadgerStore) GetDirByID(id []byte) (*syncd.Dir, bool) {
	var dir *syncd.Dir
	if err := s.db.View(func(txn *badger.Txn) error {
		var err error
		if dir, err = getDirByID(txn, id); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, false
	}
	return dir, dir != nil
}

func (s *BadgerStore) GetDirByPath(path string) (*syncd.Dir, bool) {
	var dir *syncd.Dir
	if err := s.db.View(func(txn *badger.Txn) error {
		var err error
		if dir, err = getDirByPath(txn, path); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logger.Debug(fmt.Sprintf("BadgerDB error on dir path lkp for path=%s:\n\t%s", path, err.Error()))
		return nil, false
	}
	return dir, dir != nil
}

func (s *BadgerStore) GetChainByID(id []byte) (*syncd.Chain, bool) {
	var chain *syncd.Chain
	if err := s.db.View(func(txn *badger.Txn) error {
		var err error
		if chain, err = getChainByID(txn, id); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, false
	}
	return chain, chain != nil
}

func (s *BadgerStore) GetChainByPath(dirID []byte, path string) (*syncd.Chain, bool) {
	var chain *syncd.Chain
	if err := s.db.View(func(txn *badger.Txn) error {
		chainID, err := getChainIDByPath(txn, dirID, []byte(""), path)
		if err != nil {
			return nil
		}
		if chain, err = getChainByID(txn, chainID); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, false
	}
	return chain, chain != nil
}

func (s *BadgerStore) GetChainByIno(ino uint64) (*syncd.Chain, bool) {
	var chain *syncd.Chain
	if err := s.db.View(func(txn *badger.Txn) error {
		var err error
		if chain, err = getChainByIno(txn, ino); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logger.Debug(fmt.Sprintf("BadgerDB error on chain no lkp for ino=%s:\n\t%s", ino, err.Error()))
		return nil, false
	}
	return chain, chain != nil
}

func (s *BadgerStore) GetEventByID(id []byte) (*syncd.Event, bool) {
	panic("GetEventByID not implemented")
}

func (s *BadgerStore) GetDirs() []syncd.Dir {
	dirs := make([]syncd.Dir, 0)
	if err := s.db.View(func(txn *badger.Txn) error {
		var ids [][]byte
		var err error
		prefix := []byte(PFX_DIR + ":")
		ids, dirs, err = iterPrefix[syncd.Dir](txn, prefix)
		if err != nil {
			return nil
		}
		for i := range dirs {
			dirs[i].ID = ids[i]
		}
		return nil
	}); err != nil {
		return nil
	}
	return dirs
}

func (s *BadgerStore) GetChainsInDir(id []byte) ([]syncd.Chain, bool) {
	chains := make([]syncd.Chain, 0)
	if err := s.db.View(func(txn *badger.Txn) error {
		var ids [][]byte
		var err error
		prefix := makeKey([]byte(LKP_CHAIN_DIR), id)
		ids, chains, err = iterPrefix[syncd.Chain](txn, prefix)
		if err != nil {
			return nil
		}
		for i := range chains {
			chains[i].ID = ids[i]
		}
		return nil
	}); err != nil {
		return nil, false
	}
	return chains, true
}

func (s *BadgerStore) GetEventsInChain(dirID []byte, chainID []byte) ([]syncd.Event, bool) {
	panic("GetEventsInChain not implemented")
}

func (s *BadgerStore) Close() error {
	for _, seq := range s.seqMap {
		seq.Release()
	}
	return s.db.Close()
}

func getDirByID(txn *badger.Txn, id []byte) (*syncd.Dir, error) {
	bdgDir, err := getObject[BadgerDir](txn, makeKey([]byte(PFX_DIR), id))
	if err != nil {
		return nil, err
	}
	dir := badgerDirToDir(bdgDir)
	dir.ID = id
	return dir, nil
}

func getChainByID(txn *badger.Txn, id []byte) (*syncd.Chain, error) {
	bdgChain, err := getObject[BadgerChain](txn, makeKey([]byte(PFX_CHAIN), id))
	if err != nil {
		return nil, err
	}
	chain := badgerChainToChain(bdgChain)
	chain.ID = id
	return chain, nil
}

func getDirByPath(txn *badger.Txn, path string) (*syncd.Dir, error) {
	dirID, err := getValue(txn, makeKey([]byte(LKP_DIR_PATH), []byte(path)))
	if dirID == nil || err != nil {
		return nil, err
	}
	return getDirByID(txn, dirID)
}

func getChainByIno(txn *badger.Txn, ino uint64) (*syncd.Chain, error) {
	chainID, err := getValue(txn, makeKey([]byte(LKP_CHAIN_INO), uint64ToBytes(ino)))
	if chainID == nil || err != nil {
		return nil, err
	}
	return getChainByIno(txn, ino)
}

func badgerDirToDir(badgerDir *BadgerDir) *syncd.Dir {
	return &syncd.Dir{
		Path: badgerDir.Path,
	}
}

func badgerChainToChain(badgerChain *BadgerChain) *syncd.Chain {
	return &syncd.Chain{
		Ino: badgerChain.Ino,
	}
}

func badgerEventToEvent(badgerEvent *BadgerEvent) *syncd.Event {
	var hash *string
	if badgerEvent.Hash != "" {
		hash = &badgerEvent.Hash
	}
	return &syncd.Event{
		Timestamp: badgerEvent.Timestamp,
		Path:      badgerEvent.Path,
		Type:      badgerEvent.Type,
		Size:      badgerEvent.Size,
		Hash:      hash,
		ModTime:   badgerEvent.ModTime,
	}
}
