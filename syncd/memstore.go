package syncd

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type MemDir struct {
	ID        uuid.UUID // should be generated when adding
	Path      string    // relative to configured top-level directory
	ExclGlobs []string  // exclude globs
	InclGlobs []string  // include globs
}

type MemChain struct {
	ID   uuid.UUID // id
	Dir  *MemDir   // parent dir
	Head *MemEvent // head of list
	Tail *MemEvent // pointer to most recent event
	Ino  uint64    // the ino of the syncd inode
}

type MemEvent struct {
	ID        uuid.UUID // id
	Type      string    // "create", "modify", "delete", "rename"
	Timestamp time.Time // time event processed
	Dir       *MemDir   // parent dir
	Path      string    // relative path of file
	ModTime   time.Time // modification time
	Hash      *string   // file hash (if file)
	Size      uint64    // file size
	Next      *MemEvent // next event for this node
	Prev      *MemEvent // previous event for this node
}

type DirIDMap map[uuid.UUID]*MemDir
type ChainIDMap map[uuid.UUID]*MemChain
type EventIDMap map[uuid.UUID]*MemEvent

type DirPathMap map[string]*MemDir
type ChainPathMap map[uuid.UUID]map[string]*MemChain
type EventPathMap map[uuid.UUID]map[string]*MemEvent

type ChainInoMap map[uint64]*MemChain

type MemStore struct {
	dirIDMap     DirIDMap
	dirPathMap   DirPathMap
	chainIDMap   ChainIDMap
	chainPathMap ChainPathMap
	chainInoMap  ChainInoMap
	eventIDMap   EventIDMap
	eventPathMap EventPathMap
}

func NewMemStore() *MemStore {
	return &MemStore{
		dirIDMap:     make(DirIDMap),
		dirPathMap:   make(DirPathMap),
		chainIDMap:   make(ChainIDMap),
		chainPathMap: make(ChainPathMap),
		chainInoMap:  make(ChainInoMap),
		eventIDMap:   make(EventIDMap),
		eventPathMap: make(EventPathMap),
	}
}

func (s *MemStore) AddDir(dir Dir) (*Dir, error) {
	return addDir(s, dir)
}

func (s *MemStore) AddChain(chain Chain) (*Chain, error) {
	return addChain(s, chain)
}

func (s *MemStore) AddEvent(event Event, chainID uuid.UUID) (*Event, error) {
	return addEvent(s, event, chainID)
}

func (s *MemStore) GetDirByUUID(id uuid.UUID) (*Dir, error) {
	memDir, ok := getDirByUUID(s, id)
	if !ok {
		// return nil, errors.New(fmt.Sprintf("No Dir exists w/ ID: %s", id))
		return nil, nil
	}
	return toDir(memDir), nil
}

func (s *MemStore) GetDirByPath(path string) (*Dir, error) {
	memDir, ok := getDirByPath(s, path)
	if !ok {
		// return nil, errors.New(fmt.Sprintf("No Dir exists w/ path: %s", path))
		return nil, nil
	}
	return toDir(memDir), nil
}

func (s *MemStore) GetChainByUUID(id uuid.UUID) (*Chain, error) {
	memChain, ok := getChainByUUID(s, id)
	if !ok {
		// return nil, errors.New(fmt.Sprintf("No Chain exists w/ ID: %s", id))
		return nil, nil
	}
	return toChain(memChain), nil
}

func (s *MemStore) GetChainByPath(dirID uuid.UUID, path string) (*Chain, error) {
	_, ok := getDirByUUID(s, dirID)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Cannot get chain by path, non-existant dir %s", dirID))
	}

	memChain, ok := getChainByPath(s, dirID, path)
	if !ok {
		// return nil, errors.New(fmt.Sprintf("No Chain exists for path: %s", path))
		return nil, nil
	}
	return toChain(memChain), nil
}

func (s *MemStore) GetChainByIno(ino uint64) (*Chain, error) {
	memChain, ok := getChainByIno(s, ino)
	if !ok {
		// return nil, errors.New(fmt.Sprintf("No Chain exists w/ Ino: %d", ino))
		return nil, nil
	}
	return toChain(memChain), nil
}

func (s *MemStore) GetEventByUUID(id uuid.UUID) (*Event, error) {
	memEvent, ok := getEventByUUID(s, id)
	if !ok {
		// return nil, errors.New(fmt.Sprintf("No Event exists w/ ID: %s", id))
		return nil, nil
	}
	return toEvent(memEvent), nil
}

func (s *MemStore) GetEventsInChain(id uuid.UUID) ([]Event, error) {
	memChain, ok := getChainByUUID(s, id)
	if !ok {
		return nil, errors.New(fmt.Sprintf("No Chain exists w/ ID: %s", id))
	}

	events := make([]Event, 0)

	curr := memChain.Head
	for curr != nil {
		// currEvent, err := store.GetEventByUUID(curr)
		// if err != nil {
		// 	logger.Debug(err.Error())
		// 	break
		// }
		// logger.Debug(currEvent.String())
		events = append(events, *toEvent(curr))
		curr = curr.Next
	}

	return events, nil
}

func setChainTail(s *MemStore, memChain *MemChain, memEvent *MemEvent) error {
	_, ok := s.chainPathMap[memChain.Dir.ID]
	if !ok {
		s.chainPathMap[memChain.Dir.ID] = make(map[string]*MemChain)
	}

	if memChain.Tail != nil && memChain.Tail.Path != memEvent.Path {
		// for events that change path names (renames)
		// we need this map to be up to date
		delete(s.chainPathMap[memChain.Dir.ID], memChain.Tail.Path)
	}

	s.chainPathMap[memChain.Dir.ID][memEvent.Path] = memChain

	memChain.Tail = memEvent

	return nil
}

// this function should error if:
//   - dir input has ID != uuid.Nil
//   - record exists w/ dir.Path
func addDir(s *MemStore, dir Dir) (*Dir, error) {
	if dir.ID != uuid.Nil {
		return nil, errors.New(fmt.Sprintf("Cannot add new dir, non-nil ID %s", dir))
	}
	_, ok := getDirByPath(s, dir.Path)
	if ok {
		return nil, errors.New(fmt.Sprintf("Cannot add new dir, existing record %s", dir))
	}

	memDir := toMemDir(&dir)
	memDir.ID = uuid.New()
	dir.ID = memDir.ID

	s.dirIDMap[memDir.ID] = memDir
	s.dirPathMap[memDir.Path] = memDir
	s.eventPathMap[memDir.ID] = make(map[string]*MemEvent)
	s.chainPathMap[memDir.ID] = make(map[string]*MemChain)

	return &dir, nil
}

// this function should error if:
//   - chain input has ID != uuid.Nil
//   - is orphaned by dir
//   - missing head or tail
func addChain(s *MemStore, chain Chain) (*Chain, error) {
	if chain.ID != uuid.Nil {
		return nil, errors.New(fmt.Sprintf("Cannot add new chain, non-nil ID %s", chain))
	}

	_, ok := getDirByUUID(s, chain.DirID)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Cannot add new chain, non-existant dir %s", chain.DirID))
	}

	memChain, err := toMemChain(s, &chain)
	if err != nil {
		return nil, err
	}

	memChain.ID = uuid.New()
	chain.ID = memChain.ID

	s.chainIDMap[memChain.ID] = memChain
	s.chainInoMap[memChain.Ino] = memChain

	return &chain, nil
}

func addEvent(s *MemStore, event Event, chainID uuid.UUID) (*Event, error) {
	if event.ID != uuid.Nil {
		return nil, errors.New(fmt.Sprintf("Cannot add new event, non-nil ID %s", event))
	}

	memChain, ok := getChainByUUID(s, chainID)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Cannot add new event, non-existent chain: %s", chainID))
	}

	// memDir, ok := getDirByUUID(s, event.DirID)
	// if !ok {
	// 	return nil, errors.New(fmt.Sprintf("Cannot add new event, non-existent dir %s", event))
	// }

	_, ok = s.eventPathMap[memChain.Dir.ID]
	if !ok {
		// this is a safety thing, maybe error?
		s.eventPathMap[memChain.Dir.ID] = make(map[string]*MemEvent)
	}

	memEvent, err := toMemEvent(s, &event)
	if err != nil {
		return nil, err
	}

	memEvent.ID = uuid.New()
	event.ID = memEvent.ID

	s.eventIDMap[memEvent.ID] = memEvent
	s.eventPathMap[memChain.Dir.ID][memEvent.Path] = memEvent

	if memChain.Head == nil { // first event on chain
		// Tail should also be nil
		memChain.Head = memEvent
	} else {
		memEvent.Prev = memChain.Tail
		memChain.Tail.Next = memEvent
	}

	err = setChainTail(s, memChain, memEvent)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func getDirByUUID(s *MemStore, id uuid.UUID) (*MemDir, bool) {
	memDir, ok := s.dirIDMap[id]
	return memDir, ok
}

func getChainByUUID(s *MemStore, id uuid.UUID) (*MemChain, bool) {
	memChain, ok := s.chainIDMap[id]
	return memChain, ok
}

func getChainByPath(s *MemStore, dirID uuid.UUID, path string) (*MemChain, bool) {
	memChain, ok := s.chainPathMap[dirID][path]
	return memChain, ok
}

func getChainByIno(s *MemStore, id uint64) (*MemChain, bool) {
	memChain, ok := s.chainInoMap[id]
	return memChain, ok
}

func getEventByUUID(s *MemStore, id uuid.UUID) (*MemEvent, bool) {
	memEvent, ok := s.eventIDMap[id]
	return memEvent, ok
}

func getDirByPath(s *MemStore, path string) (*MemDir, bool) {
	memDir, ok := s.dirPathMap[path]
	return memDir, ok
}

func toDir(memDir *MemDir) *Dir {
	return &Dir{
		ID:        memDir.ID,
		Path:      memDir.Path,
		ExclGlobs: memDir.ExclGlobs,
		InclGlobs: memDir.InclGlobs,
	}
}

func toMemDir(dir *Dir) *MemDir {
	return &MemDir{
		ID:        dir.ID,
		Path:      dir.Path,
		ExclGlobs: dir.ExclGlobs,
		InclGlobs: dir.InclGlobs,
	}
}

func toChain(memChain *MemChain) *Chain {
	return &Chain{
		ID:    memChain.ID,
		DirID: memChain.Dir.ID,
		// HeadID: memChain.Head.ID,
		// TailID: memChain.Tail.ID,
		Ino: memChain.Ino,
	}
}

func toMemChain(s *MemStore, chain *Chain) (*MemChain, error) {
	dir, ok := getDirByUUID(s, chain.DirID)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Cannot add chain, nonexistent parent dir: %s", chain.DirID))
	}
	// head, ok := getEventByUUID(s, chain.HeadID)
	// if !ok {
	// 	return nil, errors.New(fmt.Sprintf("Cannot add chain, nonexistent head event: %s", chain.HeadID))
	// }
	// tail, ok := getEventByUUID(s, chain.TailID)
	// if !ok {
	// 	return nil, errors.New(fmt.Sprintf("Cannot add chain, nonexistent head event: %s", chain.TailID))
	// }

	return &MemChain{
		ID:  chain.ID,
		Dir: dir,
		// Head: head,
		// Tail: tail,
		Ino: chain.Ino,
	}, nil
}

func toEvent(memEvent *MemEvent) *Event {
	// var prevID uuid.UUID
	// var nextID uuid.UUID
	// if memEvent.Prev != nil {
	// 	prevID = memEvent.Prev.ID
	// }
	// if memEvent.Next != nil {
	// 	nextID = memEvent.Next.ID
	// }
	return &Event{
		ID:        memEvent.ID,
		DirID:     memEvent.Dir.ID,
		Timestamp: time.Time{},
		Path:      memEvent.Path,
		Type:      memEvent.Type,
		Size:      memEvent.Size,
		Hash:      memEvent.Hash,
		ModTime:   memEvent.ModTime,
		// PrevID:    prevID,
		// NextID:    nextID,
	}
}

func toMemEvent(s *MemStore, event *Event) (*MemEvent, error) {
	// var prev *MemEvent
	// var next *MemEvent

	dir, ok := getDirByUUID(s, event.DirID)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Cannot add event, nonexistent parent dir: %s", event.DirID))
	}

	// if event.PrevID != uuid.Nil {
	// 	prevEv, ok := getEventByUUID(s, event.PrevID)
	// 	if !ok {
	// 		return nil, errors.New(fmt.Sprintf("Cannot add event, nonexistent prev event: %s", event.PrevID))
	// 	}
	// 	prev = prevEv
	// }
	//
	// if event.NextID != uuid.Nil {
	// 	nextEv, ok := getEventByUUID(s, event.NextID)
	// 	if !ok {
	// 		return nil, errors.New(fmt.Sprintf("Cannot add event, nonexistent next event: %s", event.NextID))
	// 	}
	// 	next = nextEv
	// }

	return &MemEvent{
		ID:        event.ID,
		Path:      event.Path,
		Type:      event.Type,
		Timestamp: event.Timestamp,
		Dir:       dir,
		ModTime:   event.ModTime,
		Hash:      event.Hash,
		Size:      event.Size,
		// Next:      next,
		// Prev:      prev,
	}, nil
}

// type MemEvent struct {
// 	ID        uuid.UUID // id
// 	Name      string    // the name of the underlying event (full path)
// 	Type      string    // "create", "modify", "delete", "rename"
// 	Timestamp time.Time // time event processed
// 	Dir       *Dir      // parent dir
// 	Path      string    // relative path of file
// 	ModTime   time.Time // modification time
// 	Hash      string    // file hash (if file)
// 	Size      uint64    // file size
// 	Next      *MemEvent // next event for this node
// 	Prev      *MemEvent // previous event for this node
// }

// type Event struct {
// 	ID        uuid.UUID  // should be generated when adding
// 	DirID     uuid.UUID  // ID of managed dir
// 	ChainID   uuid.UUID  // ID of event chain
// 	Timestamp time.Time  // timestamp of the event
// 	Path      string     // relative to DirName
// 	Type      string     // create, remove, rename, write, chmod
// 	Size      uint64     // file size
// 	Hash      *string    // file hash (null for non-files)
// 	ModTime   time.Time  // modification time
// 	Prev      *uuid.UUID // the previous event ID
// 	Next      *uuid.UUID // the next event ID
// }
