package badgerstore

import (
	"errors"
	"fmt"
	"time"

	"github.com/ceejimus/kusari/syncd"
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
const LKP_EVENT_PREV = "lkp:event:prev"
const LKP_EVENT_NEXT = "lkp:event:next"
const LKP_CHAIN_PATH_PREFIX = "lkp:chain:path"

var SEQ_KEYS = []string{PFX_DIR, PFX_CHAIN, PFX_EVENT, LKP_CHAIN_DIR}

type BadgerID []byte

type BadgerDir struct {
	ID   BadgerID // dirID
	Path string   // relative to configured top-level directory
}

type BadgerChain struct {
	ID    BadgerID // chainID
	DirID BadgerID // the dir ID to which this chain belongs
	Ino   uint64   // the ino of the syncd inode
}

type BadgerEvent struct {
	ID        BadgerID        // eventID
	ChainID   BadgerID        // the chain ID to which this event belongs
	Type      syncd.EventType // "create", "modify", "delete", "rename", etc.
	Timestamp time.Time       // time event processed
	Path      string          // relative path of file
	OldPath   string          // the old path (for create events after rename)
	ModTime   time.Time       // modification time
	Hash      string          // file hash (if file)
	Size      uint64          // file size
}

type SeqMap map[string]*badger.Sequence

type BadgerStore struct {
	db     *badger.DB
	seqMap SeqMap
}

func (id *BadgerID) Encode() []byte {
	return []byte(*id)
}

func toBadgerID(bytes []byte) (BadgerID, error) {
	if len(bytes) != 8 {
		return nil, errors.New(fmt.Sprintf("Failed to convert ID bytes to BadgerID, bytes must represent uint64: %v", bytes))
	}
	if *(*[8]byte)(bytes) == [8]byte{} {
		return nil, errors.New(fmt.Sprintln("Failed to convert ID bytes to BadgerID, ID must be greater than 0"))
	}
	return BadgerID(bytes), nil
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
		if err = addObject(txn, makeKey([]byte(PFX_DIR), toAdd.ID.Encode()), toAdd); err != nil {
			return err
		}
		// add path to dir lkp
		if err = txn.Set(makeKey([]byte(LKP_DIR_PATH), []byte(bdgDir.Path)), toAdd.ID.Encode()); err != nil {
			return err
		}
		// set the new ID
		bdgDir.ID = toAdd.ID
		return nil
	})
}

func addChain(s *BadgerStore, bdgChain *BadgerChain, dirID BadgerID) error {
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
		if err = addObject(txn, makeKey([]byte(PFX_CHAIN), toAdd.ID.Encode()), toAdd); err != nil {
			return err
		}
		// add chain to ino lkp
		if err = txn.Set(makeKey([]byte(LKP_CHAIN_INO), uint64ToBytes(bdgChain.Ino)), toAdd.ID.Encode()); err != nil {
			return err
		}
		// add chain to dir lkp
		lkpID, err := s.nextIDFor(LKP_CHAIN_DIR)
		if err != nil {
			return err
		}
		// set lookup for chain in dir
		if err = txn.Set(makeKey([]byte(LKP_CHAIN_DIR), dirID.Encode(), lkpID.Encode()), toAdd.ID.Encode()); err != nil {
			return err
		}
		// set new ID
		bdgChain.ID = toAdd.ID
		return nil
	})
}

func addEvent(s *BadgerStore, bdgEvent *BadgerEvent, chainID BadgerID) error {
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
		if err = addObject(txn, makeKey([]byte(PFX_EVENT), toAdd.ID.Encode()), toAdd); err != nil {
			return err
		}
		// get current head and tail for chain
		head, err := getChainHead(txn, chainID)
		if err != nil {
			return nil
		}
		tail, err := getChainTail(txn, chainID)
		if err != nil {
			return nil
		}
		// set head if first event, set prevEvent.Next otherwise
		if head == nil { // this is the first event in chain
			if err = setChainHead(txn, chainID, toAdd.ID); err != nil {
				return err
			}
		} else {
			if err = setEventNext(txn, tail.ID, toAdd.ID); err != nil {
				return err
			}
		}
		// update the path lookup depending on event type
		if err = updateChainLkps(txn, chainID, &toAdd, tail); err != nil {
			return err
		}
		// set the tail
		if err = setChainTail(txn, chainID, toAdd.ID); err != nil {
			return err
		}
		// set the ID
		bdgEvent.ID = toAdd.ID
		bdgEvent.OldPath = toAdd.OldPath
		return nil
	})
}

func getChainHead(txn *badger.Txn, chainID BadgerID) (*BadgerEvent, error) {
	headID, err := getID(txn, makeKey([]byte(LKP_CHAIN_HEAD), chainID.Encode()))
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

func getChainTail(txn *badger.Txn, chainID BadgerID) (*BadgerEvent, error) {
	tailID, err := getID(txn, makeKey([]byte(LKP_CHAIN_TAIL), chainID.Encode()))
	if err != nil {
		return nil, err
	}
	if tailID == nil {
		return nil, nil
	}
	event, err := getEventByID(txn, tailID)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func setChainHead(txn *badger.Txn, chainID BadgerID, headID BadgerID) error {
	return txn.Set(makeKey([]byte(LKP_CHAIN_HEAD), chainID.Encode()), headID.Encode())
}

func setChainTail(txn *badger.Txn, chainID BadgerID, tailID BadgerID) error {
	return txn.Set(makeKey([]byte(LKP_CHAIN_TAIL), chainID.Encode()), tailID.Encode())
}

func getEventNext(txn *badger.Txn, eventID BadgerID) (*BadgerEvent, error) {
	nextID, err := getID(txn, makeKey([]byte(LKP_EVENT_NEXT), eventID.Encode()))
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

func setEventNext(txn *badger.Txn, eventID BadgerID, nextID BadgerID) error {
	return txn.Set(makeKey([]byte(LKP_EVENT_NEXT), eventID.Encode()), nextID.Encode())
}

func updateChainLkps(txn *badger.Txn, chainID BadgerID, event *BadgerEvent, tail *BadgerEvent) error {
	chain, err := getObject[BadgerChain](txn, makeKey([]byte(PFX_CHAIN), chainID.Encode()))
	if err != nil {
		return err
	}
	if event.Type == syncd.Create && tail != nil && tail.Type == syncd.Rename {
		// set old path for move finalizations
		event.OldPath = tail.Path
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

func getDirByID(txn *badger.Txn, dirID BadgerID) (*BadgerDir, error) {
	dir, err := getObject[BadgerDir](txn, makeKey([]byte(PFX_DIR), dirID.Encode()))
	if err != nil {
		return nil, err
	}
	return dir, nil
}

func getChainByID(txn *badger.Txn, chainID BadgerID) (*BadgerChain, error) {
	chain, err := getObject[BadgerChain](txn, makeKey([]byte(PFX_CHAIN), chainID.Encode()))
	if err != nil {
		return nil, err
	}
	return chain, nil
}

func getEventByID(txn *badger.Txn, eventID BadgerID) (*BadgerEvent, error) {
	event, err := getObject[BadgerEvent](txn, makeKey([]byte(PFX_EVENT), eventID.Encode()))
	if err != nil {
		return nil, err
	}
	return event, nil
}

func getPrevEvent(txn *badger.Txn, eventID BadgerID) (*BadgerEvent, error) {
	event, err := getObject[BadgerEvent](txn, makeKey([]byte(PFX_EVENT), eventID.Encode()))
	if err != nil {
		return nil, err
	}
	return event, nil
}

func getDirByPath(txn *badger.Txn, path string) (*BadgerDir, error) {
	dirID, err := getID(txn, makeKey([]byte(LKP_DIR_PATH), []byte(path)))
	if dirID == nil || err != nil {
		return nil, err
	}
	return getDirByID(txn, dirID)
}

func getChainByIno(txn *badger.Txn, ino uint64) (*BadgerChain, error) {
	chainID, err := getID(txn, makeKey([]byte(LKP_CHAIN_INO), uint64ToBytes(ino)))
	if chainID == nil || err != nil {
		return nil, err
	}
	return getChainByID(txn, chainID)
}
