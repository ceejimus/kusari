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

//
// func (w *Watcher) getDirByFullPath(fullPath string) (*Dir, bool) {
// 	dir, ok := w.dirPathMap[fullPath]
// 	return dir, ok
// }

func (w *Watcher) getDirForEvent(name string) (*string, *Dir) {
	for dirPath, dir := range w.dirPathMap {
		fullPath := filepath.Join(w.topDir, dirPath)
		if strings.HasPrefix(name, fullPath) {
			return &dirPath, dir
		}
	}
	return nil, nil
}

// type WatchedNode struct {
// 	UUID uuid.UUID   // id
// 	Head *FileEvent  // head of list
// 	Tail *FileEvent  // pointer to most recent event
// 	Node *files.Node // the node we're watching
// }

func InitWatcher(topDir string, managedDirs []ManagedDirectory, managedMap ManagedMap, store EventStore) *Watcher {
	// create a new inner
	inner, err := fsnotify.NewWatcher()
	watcher := Watcher{
		inner:      inner,
		store:      store,
		topDir:     topDir,
		dirPathMap: make(map[string]*Dir),
	}
	if err != nil {
		logger.Error(err.Error())
		return nil
	}
	// add managed dirs to watcher
	for _, dir := range managedDirs {
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

		watcher.dirPathMap[newDir.Path] = newDir

		nodes := managedMap[dir.Path]
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

		err = recursiveWatcherAdd(inner, dirName)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", dirName))
			return nil
		}
		logger.Info(fmt.Sprintf("Watching %q\n", dirName))
	}

	return &watcher
}

func RunWatcher(watcher *Watcher) {
	inner := watcher.inner
	defer inner.Close()
	for {
		select {
		case event, ok := <-inner.Events:
			if !ok {
				return
			}
			// transform fsnotify event into local node event
			fileEvent := parseEvent(&event)
			if fileEvent == nil {
				continue
			}

			dirPath, dir := watcher.getDirForEvent(event.Name)
			if dirPath == nil {
				logger.Error(fmt.Sprintf("Failed to find dir for event: %s", event))
			}

			dirEvent := dirEvent{
				dirID:     dir.ID,
				dirPath:   *dirPath,
				dirName:   filepath.Join(watcher.topDir, *dirPath),
				fileEvent: fileEvent,
			}

			isNewDir, err := handleEvent(&dirEvent, watcher.store)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to handle fsnotify event:\n%v\n", err.Error()))
			}

			if isNewDir {
				logger.Trace(fmt.Sprintf("Watching new dir: %q", fileEvent.Name))
				err := recursiveWatcherAdd(inner, fileEvent.Name)
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", fileEvent.Name))
				}
			}
		case err, ok := <-inner.Errors:
			if !ok {
				return
			}
			logger.Info(fmt.Sprintf("Error received:\n%v", err.Error()))
		}
	}
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

func handleEvent(dirEvent *dirEvent, store EventStore) (bool, error) {
	fileEvent := dirEvent.fileEvent
	// lookup chain log for this event
	chain, node, err := lkpChain(dirEvent, store)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Failed to find lookup Chain for event:  %v", fileEvent))
	}
	// ignore invalid events for now
	if !isValidEvent(dirEvent, chain, node) {
		return false, errors.ErrUnsupported
	}

	// add new chain for new nodes
	if fileEvent.Type == "create" && chain == nil {
		// if we don't have a chain AND we don't have a node then something is wrong
		if node == nil {
			logger.Error(fmt.Sprintf("Failed to find node for create event %v", dirEvent))
			os.Exit(1)
		}

		newChain := &Chain{
			DirID: dirEvent.dirID,
			Ino:   node.Ino(),
		}

		newChain, err := store.AddChain(*newChain)
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}

		chain = newChain
	}

	event := Event{
		DirID:     dirEvent.dirID,
		Timestamp: fileEvent.Timestamp,
		Path:      files.GetRelativePath(fileEvent.Name, dirEvent.dirName),
		Type:      fileEvent.Type,
	}

	setEventState(&event, node)

	_, err = store.AddEvent(event, chain.ID)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	if fileEvent.Type == "remove" {
		events, err := store.GetEventsInChain(chain.ID)
		if err != nil {
			logger.Debug(err.Error())
		}
		logger.Debug("=================================")
		for _, event := range events {
			logger.Debug(fmt.Sprint(event))
		}
	}

	return fileEvent.Type == "create" && node.Type() == files.DIR, nil
}

func parseEvent(event *fsnotify.Event) *FileEvent {
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

func lkpChain(dirEvent *dirEvent, store EventStore) (*Chain, *files.Node, error) {
	fileEvent := dirEvent.fileEvent

	switch fileEvent.Type {
	case "create":
		node, err := files.NewNode(fileEvent.Name)
		if err != nil {
			return nil, node, err
		}
		chain, err := store.GetChainByIno(node.Ino())
		if err != nil {
			return nil, node, err
		}

		return chain, node, nil
	case "write":
		node, err := files.NewNode(fileEvent.Name)
		if err != nil {
			return nil, node, err
		}
		chain, err := store.GetChainByIno(node.Ino())
		if err != nil {
			return nil, node, err
		}
		if chain == nil {
			return nil, node, errors.New(fmt.Sprintf("No chain by ino on write event %+v\n%d", dirEvent, node.Ino()))
		}

		return chain, node, nil
	case "remove":
		chain, err := store.GetChainByPath(dirEvent.dirID, files.GetRelativePath(fileEvent.Name, dirEvent.dirName))
		if err != nil {
			return nil, nil, err
		}
		if chain == nil {
			return nil, nil, errors.New(fmt.Sprintf("No chain by path on remove event %+v", dirEvent))
		}

		return chain, nil, nil
	case "rename":
		chain, err := store.GetChainByPath(dirEvent.dirID, files.GetRelativePath(fileEvent.Name, dirEvent.dirName))
		if err != nil {
			return nil, nil, err
		}
		if chain == nil {
			return nil, nil, errors.New(fmt.Sprintf("No chain by path on remove event %+v", dirEvent))
		}

		return chain, nil, nil
	default:
		return nil, nil, errors.ErrUnsupported
	}
}

func isValidEvent(dirEvent *dirEvent, chain *Chain, node *files.Node) bool {
	switch dirEvent.fileEvent.Type {
	case "create":
		if node.Type() < 1 {
			return false
		}
	case "write":
		if node.Type() < 1 {
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
