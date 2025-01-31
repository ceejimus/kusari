package memstore

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"atmoscape.net/fileserver/syncd"
	"github.com/google/uuid"
)

type dirIDMap map[uuid.UUID]*MemDir
type chainIDMap map[uuid.UUID]*MemChain
type eventIDMap map[uuid.UUID]*MemEvent

type dirPathMap map[string]*MemDir

type chainMap map[string]*MemChain
type chainPathLkp map[uuid.UUID]chainMap

type chainInoMap map[uint64]*MemChain

type MemDir struct {
	ID       uuid.UUID // should be generated when adding
	Path     string    // relative to configured top-level directory
	Chains   []*MemChain
	ChainLkp chainPathLkp
}

type MemChain struct {
	ID   uuid.UUID // id
	Dir  *MemDir   // parent dir
	Head *MemEvent // head of list
	Tail *MemEvent // pointer to most recent event
	Ino  uint64    // the ino of the syncd inode
}

type MemEvent struct {
	ID        uuid.UUID       // id
	Chain     *MemChain       // the chain this event is in
	Type      syncd.EventType // "create", "modify", "delete", "rename", etc.
	Timestamp time.Time       // time event processed
	Path      string          // relative path of file
	ModTime   time.Time       // modification time
	Hash      *string         // file hash (if file)
	Size      uint64          // file size
	Next      *MemEvent       // next event for this node
	Prev      *MemEvent       // previous event for this node
}

type MemStore struct {
	mu          sync.Mutex
	dirIDMap    dirIDMap
	dirPathMap  dirPathMap
	chainIDMap  chainIDMap
	chainInoMap chainInoMap
	eventIDMap  eventIDMap
}

func (lkp chainPathLkp) get(path string) (*MemChain, bool) {
	// get parent dir path and node name
	dir, name := filepath.Split(path)
	// lookup chain map for dir
	chMap, ok := lkp.getChainMap(dir)
	if !ok {
		return nil, false
	}
	// get chain for node
	chain, ok := chMap[name]
	// return
	return chain, ok
}

func (lkp chainPathLkp) add(path string, chain *MemChain) error {
	// get parent dir path and node name
	dir, name := filepath.Split(path)
	// lookup chain map for dir
	chMap, ok := lkp.getChainMap(dir)
	if !ok {
		return errors.New(fmt.Sprintf("No chain map for: %q", dir))
	}
	// error if we already have this node
	_, ok = chMap[name]
	if ok {
		return errors.New(fmt.Sprintf("Lkp for path already exists for %q", path))
	}
	// add chain record from name -> chain
	chMap[name] = chain
	// create new chain map in lkp for node
	lkp[chain.ID] = make(chainMap)
	// return
	return nil
}

func (lkp chainPathLkp) move(dstPath string, srcPath string) error {
	// get parent dir and name for dst
	dstDir, dstName := filepath.Split(dstPath)
	// get parent dir and name for src
	srcDir, srcName := filepath.Split(srcPath)
	// lookup chain map for dst parent dir
	dstChainMap, ok := lkp.getChainMap(dstDir)
	if !ok {
		return errors.New(fmt.Sprintf("Failed to find chain map for dstPath parent dir: %q", dstDir))
	}
	// lookup chain map for src parent dir
	srcChainMap, ok := lkp.getChainMap(srcDir)
	if !ok {
		return errors.New(fmt.Sprintf("Failed to find chain map for srcPath parent dir: %q", srcDir))
	}
	// get the chain we're moving from parent and delete the key
	mvChain, ok := srcChainMap[srcName]
	if !ok {
		return errors.New(fmt.Sprintf("Failed to find chain for path: %q", dstPath))
	}
	delete(srcChainMap, srcName)
	// set the chain in the destination map
	dstChainMap[dstName] = mvChain
	// return
	return nil
}

func (lkp chainPathLkp) delete(path string) {
	// get parent dir path and node name
	dir, name := filepath.Split(path)
	// lookup chain map for dir
	chMap, ok := lkp.getChainMap(dir)
	if !ok {
		return
	}
	// delete chain record from map
	delete(chMap, name)
	// return
	return
}

func (lkp chainPathLkp) getChainMap(path string) (chainMap, bool) {
	var found chainMap
	var ok bool
	// start w/ root chain map
	found, ok = lkp[uuid.Nil]
	if !ok {
		return nil, false
	}
	// if path is empty assume they want the root node
	if path == "" {
		return found, true
	}
	// iterate over path components
	pathParts := splitPath(path)
	for _, pathPart := range pathParts {
		// find the chain for this part
		lkpChain, ok := found[pathPart]
		if !ok {
			return nil, false
		}
		// use that chain ID as lookup for its chain map
		found, ok = lkp[lkpChain.ID]
		if !ok {
			return nil, false
		}
	}
	// return
	return found, true
}

func splitPath(path string) []string {
	if dir, name := filepath.Split(filepath.Clean(path)); dir == "" {
		return []string{name}
	} else {
		return append(splitPath(dir), name)
	}
}

func NewMemStore() *MemStore {
	return &MemStore{
		dirIDMap:    make(dirIDMap),
		dirPathMap:  make(dirPathMap),
		chainIDMap:  make(chainIDMap),
		chainInoMap: make(chainInoMap),
		eventIDMap:  make(eventIDMap),
	}
}

func (s *MemStore) AddDir(dir syncd.Dir) (*syncd.Dir, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	memDir := MemDir{Path: dir.Path}
	if err := addDir(s, &memDir); err != nil {
		return nil, err
	}
	newDir := toDir(&memDir)
	return newDir, nil
}

func (s *MemStore) AddChain(chain syncd.Chain, dirID []byte) (*syncd.Chain, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dirUUID, err := uuid.FromBytes(dirID)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Cannot add new chain, cannot parse dirID to UUID: %v", dirID))
	}
	memChain := MemChain{Ino: chain.Ino}
	if err := addChain(s, &memChain, dirUUID); err != nil {
		return nil, err
	}
	newChain := toChain(&memChain)
	return newChain, nil
}

func (s *MemStore) AddEvent(event syncd.Event, chainID []byte) (*syncd.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chainUUID, err := uuid.FromBytes(chainID)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Cannot add event, cannot parse chainID to UUID: %v", chainID))
	}

	memEvent := MemEvent{
		Path:      event.Path,
		Type:      event.Type,
		Timestamp: event.Timestamp,
		ModTime:   event.ModTime,
		Hash:      event.Hash,
		Size:      event.Size,
	}
	if err := addEvent(s, &memEvent, chainUUID); err != nil {
		return nil, err
	}
	newEvent := toEvent(&memEvent)
	return newEvent, nil
}

func (s *MemStore) GetDirByID(id []byte) (*syncd.Dir, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dirUUID, err := uuid.FromBytes(id)
	if err != nil {
		return nil, false
	}
	memDir, ok := getDirByUUID(s, dirUUID)
	if !ok {
		return nil, false
	}
	return toDir(memDir), true
}

func (s *MemStore) GetDirByPath(path string) (*syncd.Dir, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	memDir, ok := getDirByPath(s, path)
	if !ok {
		return nil, false
	}
	return toDir(memDir), true
}

func (s *MemStore) GetChainByID(id []byte) (*syncd.Chain, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chainUUID, err := uuid.FromBytes(id)
	if err != nil {
		return nil, false
	}
	memChain, ok := getChainByID(s, chainUUID)
	if !ok {
		return nil, false
	}
	return toChain(memChain), true
}

func (s *MemStore) GetChainByPath(dirID []byte, path string) (*syncd.Chain, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dirUUID, err := uuid.FromBytes(dirID)
	if err != nil {
		return nil, false
	}
	_, ok := getDirByUUID(s, dirUUID)
	if !ok {
		return nil, false
	}

	memChain, ok := getChainByPath(s, dirUUID, path)
	if !ok {
		return nil, false
	}
	return toChain(memChain), true
}

func (s *MemStore) GetChainByIno(ino uint64) (*syncd.Chain, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	memChain, ok := getChainByIno(s, ino)
	if !ok {
		return nil, false
	}
	return toChain(memChain), true
}

func (s *MemStore) GetEventByID(id []byte) (*syncd.Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	eventUUID, err := uuid.FromBytes(id)
	if err != nil {
		return nil, false
	}
	memEvent, ok := getEventByUUID(s, eventUUID)
	if !ok {
		return nil, false
	}
	return toEvent(memEvent), true
}

func (s *MemStore) GetEventsInChain(id []byte) ([]syncd.Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chainUUID, err := uuid.FromBytes(id)
	if err != nil {
		return nil, false
	}
	memChain, ok := getChainByID(s, chainUUID)
	if !ok {
		return []syncd.Event{}, false
	}

	events := make([]syncd.Event, 0)

	curr := memChain.Head
	for curr != nil {
		events = append(events, *toEvent(curr))
		curr = curr.Next
	}

	return events, true
}

func (s *MemStore) GetDirs() []syncd.Dir {
	s.mu.Lock()
	defer s.mu.Unlock()
	dirs := make([]syncd.Dir, len(s.dirIDMap))
	i := 0
	for _, memDir := range s.dirIDMap {
		dirs[i] = *toDir(memDir)
		i++
	}
	return dirs
}

func (s *MemStore) GetChainsInDir(id []byte) ([]syncd.Chain, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dirUUID, err := uuid.FromBytes(id)
	if err != nil {
		return nil, false
	}
	memDir, ok := getDirByUUID(s, dirUUID)
	if !ok {
		return nil, false
	}

	chains := make([]syncd.Chain, len(memDir.Chains))

	for i, memChain := range memDir.Chains {
		chains[i] = *toChain(memChain)
	}

	return chains, true
}

func (s *MemStore) Close() error { return nil } // nothing to clean up

// this function should error if:
//   - dir input has ID != uuid.Nil
//   - record exists w/ dir.Path
func addDir(s *MemStore, memDir *MemDir) error {
	if memDir.ID != uuid.Nil {
		return errors.New(fmt.Sprintf("Cannot add new dir, non-nil ID %v", memDir))
	}
	_, ok := getDirByPath(s, memDir.Path)
	if ok {
		return errors.New(fmt.Sprintf("Cannot add new dir, existing record %v", memDir))
	}

	memDir.ID = uuid.New()
	memDir.Chains = make([]*MemChain, 0)
	memDir.ChainLkp = make(chainPathLkp)
	memDir.ChainLkp[uuid.Nil] = make(chainMap)

	s.dirIDMap[memDir.ID] = memDir
	s.dirPathMap[memDir.Path] = memDir

	return nil
}

// this function should error if:
//   - chain input has ID != uuid.Nil
//   - is orphaned by dir
//   - missing head or tail
func addChain(s *MemStore, memChain *MemChain, dirID uuid.UUID) error {
	if memChain.ID != uuid.Nil {
		return errors.New(fmt.Sprintf("Cannot add new chain, non-nil ID %v", memChain))
	}

	dir, ok := getDirByUUID(s, dirID)
	if !ok {
		return errors.New(fmt.Sprintf("Cannot add new chain, non-existent dir %s", dirID))
	}

	memChain.ID = uuid.New()
	memChain.Dir = dir

	s.chainIDMap[memChain.ID] = memChain
	s.chainInoMap[memChain.Ino] = memChain

	dir.Chains = append(dir.Chains, memChain)

	return nil
}

func addEvent(s *MemStore, memEvent *MemEvent, chainID uuid.UUID) error {
	if memEvent.ID != uuid.Nil {
		return errors.New(fmt.Sprintf("Cannot add new event, non-nil ID %v", memEvent))
	}

	memChain, ok := getChainByID(s, chainID)
	if !ok {
		return errors.New(fmt.Sprintf("Cannot add new event, non-existent chain: %s", chainID))
	}

	memEvent.ID = uuid.New()
	memEvent.Chain = memChain

	s.eventIDMap[memEvent.ID] = memEvent

	if memChain.Head == nil { // first event on chain
		// Tail should also be nil
		memChain.Head = memEvent
	} else {
		memEvent.Prev = memChain.Tail
		memChain.Tail.Next = memEvent
	}

	err := updateChainLkps(s, memChain, memEvent)
	if err != nil {
		return err
	}

	memChain.Tail = memEvent

	return nil
}

func updateChainLkps(s *MemStore, memChain *MemChain, memEvent *MemEvent) error {
	// sprinkle some fairy dust for path lookups after renames
	if memEvent.Type == syncd.Create && memChain.Tail != nil && memChain.Tail.Type == syncd.Rename {
		if err := memChain.Dir.ChainLkp.move(memEvent.Path, memChain.Tail.Path); err != nil {
			return err
		}
	} else if memEvent.Type == syncd.Remove {
		delete(s.chainInoMap, memChain.Ino)
		memChain.Dir.ChainLkp.delete(memEvent.Path)
	} else {
		if _, ok := memChain.Dir.ChainLkp.get(memEvent.Path); !ok {
			if err := memChain.Dir.ChainLkp.add(memEvent.Path, memChain); err != nil {
				return err
			}
		}
	}

	return nil
}

func getDirByUUID(s *MemStore, id uuid.UUID) (*MemDir, bool) {
	memDir, ok := s.dirIDMap[id]
	return memDir, ok
}

func getChainByID(s *MemStore, id uuid.UUID) (*MemChain, bool) {
	memChain, ok := s.chainIDMap[id]
	return memChain, ok
}

func getChainByPath(s *MemStore, dirID uuid.UUID, path string) (*MemChain, bool) {
	dir, ok := s.dirIDMap[dirID]
	if !ok {
		return nil, false
	}
	memChain, ok := dir.ChainLkp.get(path)
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

func toDir(memDir *MemDir) *syncd.Dir {
	return &syncd.Dir{
		ID:   memDir.ID[:],
		Path: memDir.Path,
	}
}

func toChain(memChain *MemChain) *syncd.Chain {
	return &syncd.Chain{
		ID:  memChain.ID[:],
		Ino: memChain.Ino,
	}
}

func toEvent(memEvent *MemEvent) *syncd.Event {
	return &syncd.Event{
		ID:        memEvent.ID[:],
		Timestamp: time.Time{},
		Path:      memEvent.Path,
		Type:      memEvent.Type,
		Size:      memEvent.Size,
		Hash:      memEvent.Hash,
		ModTime:   memEvent.ModTime,
	}
}
