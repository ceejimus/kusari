package syncd

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

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

type MemStore struct {
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

func (s *MemStore) GetDirs() []Dir {
	dirs := make([]Dir, len(s.dirIDMap))
	i := 0
	for _, memDir := range s.dirIDMap {
		dirs[i] = *toDir(memDir)
		i++
	}
	return dirs
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
	memDir.ChainLkp = make(chainPathLkp)
	memDir.ChainLkp[uuid.Nil] = make(chainMap)

	dir.ID = memDir.ID

	s.dirIDMap[memDir.ID] = memDir
	s.dirPathMap[memDir.Path] = memDir

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

	memEvent, err := toMemEvent(&event)
	if err != nil {
		return nil, err
	}

	memEvent.ID = uuid.New()
	memEvent.Chain = memChain

	event.ID = memEvent.ID

	s.eventIDMap[memEvent.ID] = memEvent

	if memChain.Head == nil { // first event on chain
		// Tail should also be nil
		memChain.Head = memEvent
	} else {
		memEvent.Prev = memChain.Tail
		memChain.Tail.Next = memEvent
	}

	err = updateChainLkps(s, memChain, memEvent)
	if err != nil {
		return nil, err
	}

	memChain.Tail = memEvent

	return &event, nil
}

func updateChainLkps(s *MemStore, memChain *MemChain, memEvent *MemEvent) error {
	// sprinkle some fairy dust for path lookups after renames
	if memEvent.Type == "create" && memChain.Tail != nil && memChain.Tail.Type == "rename" {
		if err := memChain.Dir.ChainLkp.move(memEvent.Path, memChain.Tail.Path); err != nil {
			return err
		}
	} else if memEvent.Type == "remove" {
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

func getChainByUUID(s *MemStore, id uuid.UUID) (*MemChain, bool) {
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
