package badgerstore

import (
	"errors"
	"fmt"
	"time"

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

type BadgerDir struct {
	ID   []byte // dirID
	Path string // relative to configured top-level directory
}

type BadgerChain struct {
	ID    []byte // chainID
	DirID []byte // the dir ID to which this chain belongs
	Ino   uint64 // the ino of the syncd inode
}

type BadgerEvent struct {
	ID        []byte          // eventID
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

func addDir(s *BadgerStore, bdgDir *BadgerDir) error {
	return s.db.Update(func(txn *badger.Txn) error {
		existingDir, err := getDirByPath(txn, bdgDir.Path)
		if err != nil {
			return err
		}
		if existingDir != nil {
			return errors.New(fmt.Sprintf("Cannot add Dir, existing dir w/ path %s", bdgDir.Path))
		}

		// add new dir
		// we copy the value since we don't want to set the ID unless txn goes thru
		toAdd := *bdgDir
		if toAdd.ID, err = s.nextIDFor(PFX_DIR); err != nil {
			return err
		}
		if err = addObject(txn, makeKey([]byte(PFX_DIR), toAdd.ID), toAdd); err != nil {
			return err
		}
		// add path to dir lkp
		if err = txn.Set(makeKey([]byte(LKP_DIR_PATH), []byte(bdgDir.Path)), toAdd.ID); err != nil {
			return err
		}
		// set the new ID
		bdgDir.ID = toAdd.ID
		return nil
	})
}

func addChain(s *BadgerStore, bdgChain *BadgerChain, dirID []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// check dir exists
		dir, err := getDirByID(txn, dirID)
		if dir == nil || err != nil {
			return errors.New(fmt.Sprintf("Cannot add new chain, nonexistent dir w/ id: %v", dirID))
		}
		// NOTE: at this point we might to do an ino lkp check, however it's possible for inodes to get reused
		// this would start a new chain, we just need to be aware of this

		// add new chain
		toAdd := *bdgChain
		if toAdd.ID, err = s.nextIDFor(PFX_CHAIN); err != nil {
			return err
		}
		if err = addObject(txn, makeKey([]byte(PFX_CHAIN), toAdd.ID), toAdd); err != nil {
			return err
		}
		// add chain to ino lkp
		if err = txn.Set(makeKey([]byte(LKP_CHAIN_INO), uint64ToBytes(bdgChain.Ino)), toAdd.ID); err != nil {
			return err
		}
		// add chain to dir lkp
		lkpID, err := s.nextIDFor(LKP_CHAIN_DIR)
		if err != nil {
			return err
		}
		// set lookup for chain in dir
		if err = txn.Set(makeKey([]byte(LKP_CHAIN_DIR), dirID, lkpID), toAdd.ID); err != nil {
			return err
		}
		// set new ID
		bdgChain.ID = toAdd.ID
		return nil
	})
}

func addEvent(s *BadgerStore, bdgEvent *BadgerEvent, chainID []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// check chain exists
		chain, err := getChainByID(txn, chainID)
		if chain == nil || err != nil {
			return errors.New(fmt.Sprintf("Cannot add new event, nonexistent chain w/ id: %v", chainID))
		}
		// add event
		toAdd := *bdgEvent
		if toAdd.ID, err = s.nextIDFor(PFX_EVENT); err != nil {
			return err
		}
		if err = addObject(txn, makeKey([]byte(PFX_EVENT), toAdd.ID), toAdd); err != nil {
			return err
		}
		// get current head and tail for chain
		head, err := getChainHead(txn, chainID)
		if err != nil {
			return nil
		}
		tailID, err := getValue(txn, makeKey([]byte(LKP_CHAIN_TAIL), chainID))
		if err != nil {
			return nil
		}
		// set head if first event, set prevEvent.Next otherwise
		if head == nil { // this is the first event in chain
			if err = setChainHead(txn, chainID, toAdd.ID); err != nil {
				return err
			}
		} else {
			if err = setEventNext(txn, tailID, toAdd.ID); err != nil {
				return err
			}
		}
		// update the path lookup depending on event type
		if err = updateChainLkps(txn, chainID, toAdd, tailID); err != nil {
			return err
		}
		// set the tail
		if err = setChainTail(txn, chainID, toAdd.ID); err != nil {
			return err
		}
		// set the ID
		bdgEvent.ID = toAdd.ID
		return nil
	})
}

func getChainHead(txn *badger.Txn, chainID []byte) (*BadgerEvent, error) {
	headID, err := getValue(txn, makeKey([]byte(LKP_CHAIN_HEAD), chainID))
	if err != nil {
		return nil, err
	}
	if headID == nil {
		return nil, nil
	}
	event, err := getEventByID(txn, headID)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func setChainHead(txn *badger.Txn, chainID []byte, headID []byte) error {
	return txn.Set(makeKey([]byte(LKP_CHAIN_HEAD), chainID), headID)
}

func setChainTail(txn *badger.Txn, chainID []byte, tailID []byte) error {
	return txn.Set(makeKey([]byte(LKP_CHAIN_TAIL), chainID), tailID)
}

func getEventNext(txn *badger.Txn, eventID []byte) (*BadgerEvent, error) {
	nextID, err := getValue(txn, makeKey([]byte(LKP_EVENT_NEXT), eventID))
	if err != nil {
		return nil, err
	}
	if nextID == nil {
		return nil, nil
	}
	event, err := getEventByID(txn, nextID)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func setEventNext(txn *badger.Txn, eventID []byte, nextID []byte) error {
	return txn.Set(makeKey([]byte(LKP_EVENT_NEXT), eventID), nextID)
}

func updateChainLkps(txn *badger.Txn, chainID []byte, event BadgerEvent, tailID []byte) error {
	var tail *BadgerEvent
	chain, err := getObject[BadgerChain](txn, makeKey([]byte(PFX_CHAIN), chainID))
	if err != nil {
		return err
	}
	if tailID != nil {
		tail, err = getObject[BadgerEvent](txn, makeKey([]byte(PFX_EVENT), tailID))
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
		existingChainID, err := getChainIDByPath(txn, chain.DirID, event.Path)
		if err != nil {
			return err
		}
		if existingChainID == nil {
			if err := addChainPathLkp(txn, chain.DirID, event.Path, chainID); err != nil {
				return err
			}
		}
	}

	return nil
}

func getDirByID(txn *badger.Txn, id []byte) (*BadgerDir, error) {
	dir, err := getObject[BadgerDir](txn, makeKey([]byte(PFX_DIR), id))
	if err != nil {
		return nil, err
	}
	return dir, nil
}

func getChainByID(txn *badger.Txn, id []byte) (*BadgerChain, error) {
	chain, err := getObject[BadgerChain](txn, makeKey([]byte(PFX_CHAIN), id))
	if err != nil {
		return nil, err
	}
	return chain, nil
}

func getEventByID(txn *badger.Txn, id []byte) (*BadgerEvent, error) {
	event, err := getObject[BadgerEvent](txn, makeKey([]byte(PFX_EVENT), id))
	if err != nil {
		return nil, err
	}
	return event, nil
}

func getDirByPath(txn *badger.Txn, path string) (*BadgerDir, error) {
	dirID, err := getValue(txn, makeKey([]byte(LKP_DIR_PATH), []byte(path)))
	if dirID == nil || err != nil {
		return nil, err
	}
	return getDirByID(txn, dirID)
}

func getChainByIno(txn *badger.Txn, ino uint64) (*BadgerChain, error) {
	chainID, err := getValue(txn, makeKey([]byte(LKP_CHAIN_INO), uint64ToBytes(ino)))
	if chainID == nil || err != nil {
		return nil, err
	}
	return getChainByID(txn, chainID)
}
