package syncd

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type MemDir struct {
	ID     uuid.UUID // should be generated when adding
	Path   string    // relative to configured top-level directory
	Chains []*MemChain
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
	Chain     *MemChain
	Type      string    // "create", "modify", "delete", "rename"
	Timestamp time.Time // time event processed
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

func (s *MemStore) AddChain(chain Chain, dirID uuid.UUID) (*Chain, error) {
	return addChain(s, chain, dirID)
}

func (s *MemStore) AddEvent(event Event, chainID uuid.UUID) (*Event, error) {
	return addEvent(s, event, chainID)
}

func (s *MemStore) GetDirByUUID(id uuid.UUID) (*Dir, bool) {
	memDir, ok := getDirByUUID(s, id)
	if !ok {
		return nil, false
	}
	return toDir(memDir), true
}

func (s *MemStore) GetDirByPath(path string) (*Dir, bool) {
	memDir, ok := getDirByPath(s, path)
	if !ok {
		return nil, false
	}
	return toDir(memDir), true
}

func (s *MemStore) GetChainByUUID(id uuid.UUID) (*Chain, bool) {
	memChain, ok := getChainByUUID(s, id)
	if !ok {
		return nil, false
	}
	return toChain(memChain), true
}

func (s *MemStore) GetChainByPath(dirID uuid.UUID, path string) (*Chain, bool) {
	_, ok := getDirByUUID(s, dirID)
	if !ok {
		return nil, false
	}

	memChain, ok := getChainByPath(s, dirID, path)
	if !ok {
		return nil, false
	}
	return toChain(memChain), true
}

func (s *MemStore) GetChainByIno(ino uint64) (*Chain, bool) {
	memChain, ok := getChainByIno(s, ino)
	if !ok {
		return nil, false
	}
	return toChain(memChain), true
}

func (s *MemStore) GetEventByUUID(id uuid.UUID) (*Event, bool) {
	memEvent, ok := getEventByUUID(s, id)
	if !ok {
		return nil, false
	}
	return toEvent(memEvent), true
}

func (s *MemStore) GetEventsInChain(id uuid.UUID) ([]Event, bool) {
	memChain, ok := getChainByUUID(s, id)
	if !ok {
		return []Event{}, false
	}

	events := make([]Event, 0)

	curr := memChain.Head
	for curr != nil {
		events = append(events, *toEvent(curr))
		curr = curr.Next
	}

	return events, true
}

func (s *MemStore) GetChainsInDir(id uuid.UUID) ([]Chain, bool) {
	memDir, ok := getDirByUUID(s, id)
	if !ok {
		return nil, false
	}

	chains := make([]Chain, len(memDir.Chains))

	for i, memChain := range memDir.Chains {
		chains[i] = *toChain(memChain)
	}

	return chains, true
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
	memDir.Chains = make([]*MemChain, 0)

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
func addChain(s *MemStore, chain Chain, dirID uuid.UUID) (*Chain, error) {
	if chain.ID != uuid.Nil {
		return nil, errors.New(fmt.Sprintf("Cannot add new chain, non-nil ID %s", chain))
	}

	dir, ok := getDirByUUID(s, dirID)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Cannot add new chain, non-existent dir %s", dirID))
	}

	memChain, err := toMemChain(&chain)
	if err != nil {
		return nil, err
	}

	memChain.ID = uuid.New()
	memChain.Dir = dir

	chain.ID = memChain.ID

	s.chainIDMap[memChain.ID] = memChain
	s.chainInoMap[memChain.Ino] = memChain

	dir.Chains = append(dir.Chains, memChain)

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

	_, ok = s.eventPathMap[memChain.Dir.ID]
	if !ok {
		// this is a safety thing, maybe error?
		s.eventPathMap[memChain.Dir.ID] = make(map[string]*MemEvent)
	}

	memEvent, err := toMemEvent(&event)
	if err != nil {
		return nil, err
	}

	memEvent.ID = uuid.New()
	memEvent.Chain = memChain

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
		ID:   memDir.ID,
		Path: memDir.Path,
	}
}

func toMemDir(dir *Dir) *MemDir {
	return &MemDir{
		ID:   dir.ID,
		Path: dir.Path,
	}
}

func toChain(memChain *MemChain) *Chain {
	return &Chain{
		ID:  memChain.ID,
		Ino: memChain.Ino,
	}
}

func toMemChain(chain *Chain) (*MemChain, error) {
	return &MemChain{
		ID:  chain.ID,
		Ino: chain.Ino,
	}, nil
}

func toEvent(memEvent *MemEvent) *Event {
	return &Event{
		ID:        memEvent.ID,
		Timestamp: time.Time{},
		Path:      memEvent.Path,
		Type:      memEvent.Type,
		Size:      memEvent.Size,
		Hash:      memEvent.Hash,
		ModTime:   memEvent.ModTime,
	}
}

func toMemEvent(event *Event) (*MemEvent, error) {
	return &MemEvent{
		ID:        event.ID,
		Path:      event.Path,
		Type:      event.Type,
		Timestamp: event.Timestamp,
		ModTime:   event.ModTime,
		Hash:      event.Hash,
		Size:      event.Size,
	}, nil
}
