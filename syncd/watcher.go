package syncd

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	files "atmoscape.net/fileserver/fs"
	"atmoscape.net/fileserver/logger"
	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

type Watcher struct {
	inner      *fsnotify.Watcher
	store      EventStore
	topDir     string
	dirPathMap map[string]*Dir
}

type dirEvent struct {
	dirID     uuid.UUID
	dirPath   string // full path
	dirName   string // relative path
	node      *files.Node
	chain     *Chain
	fileEvent *FileEvent
}

// TODO: make "enum"
type FileEvent struct {
	Name      string           // the name of the underlying event (full path)
	Type      string           // "create", "modify", "delete", "rename"
	Timestamp time.Time        // time event processed
	State     *files.NodeState // node state
	Next      *FileEvent       // next event for this node
	Prev      *FileEvent       // previous event for this node
}

func (w *Watcher) getDirForEvent(name string) (*string, *Dir) {
	for dirPath, dir := range w.dirPathMap {
		fullPath := filepath.Join(w.topDir, dirPath)
		if strings.HasPrefix(name, fullPath) {
			return &dirPath, dir
		}
	}
	return nil, nil
}

func InitWatcher(topDir string, managedDirs []ManagedDirectory, managedMap ManagedMap, store EventStore) (*Watcher, error) {
	// create a new inner
	inner, err := fsnotify.NewWatcher()
	watcher := Watcher{
		inner:      inner,
		store:      store,
		topDir:     topDir,
		dirPathMap: make(map[string]*Dir),
	}
	if err != nil {
		return nil, err
	}
	// add managed dirs to watcher
	for _, dir := range managedDirs {
		// update event store w/ current directory state
		nodes := managedMap[dir.Path]
		newDir, err := updateStoreForLocalState(topDir, dir, nodes, store)
		if err != nil {
			return nil, err
		}
		// add directory to path map for event lookups
		watcher.dirPathMap[dir.Path] = newDir
		// recursively add watcher to the directory and all sub-directories
		err = recursiveWatcherAdd(inner, filepath.Join(topDir, dir.Path))
		if err != nil {
			return nil, err
		}
		logger.Info(fmt.Sprintf("Watching %q\n", dir.Path))
	}

	return &watcher, nil
}

func RunWatcher(watcher *Watcher) {
	inner := watcher.inner
	defer inner.Close()
	// run loop
	for {
		select {
		case event, ok := <-inner.Events:
			if !ok { // channel closed
				return
			}
			// transform fsnotify event into local node event
			fileEvent := toFileEvent(&event)
			if fileEvent == nil { // ignored event
				continue
			}
			// lookup managed directory by event name
			dirPath, dir := watcher.getDirForEvent(event.Name)
			if dirPath == nil {
				logger.Error(fmt.Sprintf("Failed to find dir for event: %s", event))
			}
			// create new event wrapper
			dirEvent := dirEvent{
				dirID:     dir.ID,
				dirPath:   *dirPath,
				dirName:   filepath.Join(watcher.topDir, *dirPath),
				fileEvent: fileEvent,
			}
			// handle this event
			err := handleEvent(&dirEvent, watcher.store)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to handle fsnotify event:\n%v\n", err.Error()))
			}
			// add new directories to watcher
			if fileEvent.Type == "create" && dirEvent.node.Type() == files.DIR {
				logger.Trace(fmt.Sprintf("Watching new dir: %q", fileEvent.Name))
				// TODO: is it necessary to do recursive calls here?
				err := recursiveWatcherAdd(inner, fileEvent.Name)
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", fileEvent.Name))
				}
			}
		case err, ok := <-inner.Errors:
			if !ok { // channel closed
				return
			}
			logger.Info(fmt.Sprintf("Error received:\n%v", err.Error()))
		}
	}
}

func updateStoreForLocalState(topDir string, dir ManagedDirectory, nodes []files.Node, store EventStore) (*Dir, error) {
	// TODO: ignore events based on globs
	dirName := filepath.Join(topDir, dir.Path)

	newDir := &Dir{
		Path:      dir.Path,
		ExclGlobs: dir.Exclude,
		InclGlobs: dir.Include,
	}
	newDir, err := store.AddDir(*newDir)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to add dir to event store:\n%s", err.Error()))
		os.Exit(1)
	}

	// populate event logs w/ pseudos based on current state
	for _, node := range nodes {
		state := node.State()
		event := &Event{
			DirID:     newDir.ID,
			Timestamp: time.Now(),
			Path:      files.GetRelativePath(node.Path, dirName),
			Type:      "create",
			Size:      state.Size,
			Hash:      state.Hash,
			ModTime:   state.ModTime,
		}

		chain := &Chain{
			DirID: newDir.ID,
			Ino:   node.Ino(),
		}

		newChain, err := store.AddChain(*chain)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to add event to event store:\n%s", err.Error()))
			os.Exit(1)
		}

		_, err = store.AddEvent(*event, newChain.ID)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to add event to event store:\n%s", err.Error()))
			os.Exit(1)
		}
	}

	return newDir, nil
}

func recursiveWatcherAdd(watcher *fsnotify.Watcher, path string) error {
	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.Type().IsDir() {
			return nil
		}

		watcher.Add(path)
		return nil
	})

	return err
}

func handleEvent(dirEvent *dirEvent, store EventStore) error {
	var err error

	fileEvent := dirEvent.fileEvent
	// set node info on event
	err = setNode(dirEvent)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to set node for: %v", dirEvent))
	}
	// lookup chain log for this event
	err = lkpChain(dirEvent, store)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to find lookup Chain for event:  %v", fileEvent))
	}
	// ignore invalid events for now
	if !isValidEvent(dirEvent) {
		return errors.ErrUnsupported
	}
	// add new chain for new nodes
	if fileEvent.Type == "create" && dirEvent.chain == nil {
		// create and new chain
		dirEvent.chain, err = store.AddChain(Chain{
			DirID: dirEvent.dirID,
			Ino:   dirEvent.node.Ino(),
		})
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
	}
	// create new event to store
	event := Event{
		DirID:     dirEvent.dirID,
		Timestamp: fileEvent.Timestamp,
		Path:      files.GetRelativePath(fileEvent.Name, dirEvent.dirName),
		Type:      fileEvent.Type,
	}
	// set event state from node
	setEventState(&event, dirEvent.node)
	// add event to store
	_, err = store.AddEvent(event, dirEvent.chain.ID)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	// for debug
	if fileEvent.Type == "remove" {
		events, err := store.GetEventsInChain(dirEvent.chain.ID)
		if err != nil {
			logger.Debug(err.Error())
		}
		logger.Debug("========  CHAIN START =============")
		for _, event := range events {
			logger.Debug(fmt.Sprint(event))
		}
		logger.Debug("=========  CHAIN END  ============")
	}

	return nil
}

// toFileEvent constructs a FileEvent from an fsnotify.Event
// it add a timestamp and returns nil if this isn't an event we care about
func toFileEvent(event *fsnotify.Event) *FileEvent {
	// init fileEvent
	fileEvent := FileEvent{
		Name:      event.Name,
		Timestamp: time.Now(),
	}
	// set internal type
	switch {
	case eventIs(fsnotify.Create, event):
		fileEvent.Type = "create"
	case eventIs(fsnotify.Write, event):
		fileEvent.Type = "write"
	case eventIs(fsnotify.Remove, event):
		fileEvent.Type = "remove"
	case eventIs(fsnotify.Rename, event):
		fileEvent.Type = "rename"
	default:
		return nil
	}

	return &fileEvent
}

// setNode sets the node property on the dirEvent struct
// the node is used to: detect new dirs, lookup chains by ino, set event state
func setNode(dirEvent *dirEvent) error {
	switch dirEvent.fileEvent.Type {
	case "create", "write":
		node, err := files.NewNode(dirEvent.fileEvent.Name)
		if err != nil {
			return err
		}
		dirEvent.node = node
	case "rename", "remove":
		return nil
	}
	return errors.ErrUnsupported
}

// lkpChain finds a chain in the event store for a given event
// it uses inode lookup for creates/writes and paths for rename/remove
// in order to perform the inode lookup, lkpChain must go to the filesystem anyways
// so this function is also responsible for retrieving and returning node state
func lkpChain(dirEvent *dirEvent, store EventStore) error {
	var chain *Chain
	var err error

	switch dirEvent.fileEvent.Type {
	case "create", "write":
		chain, err = store.GetChainByIno(dirEvent.node.Ino())
		if err != nil {
			return err
		}
	case "remove", "rename":
		chain, err = store.GetChainByPath(
			dirEvent.dirID,
			files.GetRelativePath(dirEvent.fileEvent.Name, dirEvent.dirName),
		)
		if err != nil {
			return err
		}
	default:
		return errors.ErrUnsupported
	}
	dirEvent.chain = chain
	return nil
}

// isValidEvent ensures dirEvent is properly initialized for processing
func isValidEvent(dirEvent *dirEvent) bool {
	node := dirEvent.node
	chain := dirEvent.chain
	switch dirEvent.fileEvent.Type {
	case "create":
		if node == nil || node.Type() < 1 {
			return false
		}
	case "write":
		if node == nil || node.Type() < 1 {
			return false
		}
		if chain == nil {
			return false
		}
	case "rename":
		if chain == nil {
			return false
		}
	case "remove":
		if chain == nil {
			return false
		}
	default:
		return false
	}
	return true
}

func setEventState(event *Event, node *files.Node) {
	switch event.Type {
	case "create":
		logger.Trace(fmt.Sprintf("Create event: %s", node.String()))
		// set event node state props
		nodeState := node.State()
		event.ModTime = node.ModTime()
		event.Size = node.Size()
		event.Hash = nodeState.Hash
	case "write":
		logger.Trace(fmt.Sprintf("Write event: %s", node.String()))
		// set event node state props
		nodeState := node.State()
		event.ModTime = node.ModTime()
		event.Size = node.Size()
		event.Hash = nodeState.Hash
	default:
	}
}

func eventIs(op fsnotify.Op, event *fsnotify.Event) bool {
	return event.Op&op == op
}
