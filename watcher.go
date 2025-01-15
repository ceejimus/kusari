package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

// TODO: make "enum"
type FileEvent struct {
	Type      string       // "create", "modify", "delete", "rename"
	Timestamp time.Time    // time event processed
	State     *NodeState   // node state
	Watched   *WatchedNode // pointer to parent watched node
}

type WatchedNode struct {
	UUID   uuid.UUID    // id
	Log    []*FileEvent // events for this file
	Latest *FileEvent   // pointer to most recent event
	Meta   *NodeMeta    // node metadata
	// Node   Node         // actual node
}

// for create / write we can (and should) find watched via inode
type WatchedInodeMap map[uint64]*WatchedNode

// for rename / remove we don't have an inode anymore and can find by path
type WatchedPathMap map[string]*WatchedNode

type RenameEvents map[uint64]*FileEvent // map nodes that got rename events

var watchedInodeMap = make(WatchedInodeMap)
var watchedPathMap = make(WatchedPathMap)
var renameEvents = make(RenameEvents) // TODO clean stale entries

var managedDirMap = make(map[string]ManagedDirectory)

func initWatcher(config *NodeConfig) *fsnotify.Watcher {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	for _, dir := range config.ManagedDirectories {
		// TODO: setup watch on all subdirectories
		// TODO: ignore events based on globs
		dirPath := filepath.Join(config.TopDir, dir.Path)
		err = watcher.Add(dirPath)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", dirPath))
			return nil
		}
		logger.Info(fmt.Sprintf("Watching %q\n", dirPath))
		managedDirMap[dirPath] = dir // TODO: does this copy underlying array, maybe don't worry about it
	}

	return watcher
}

func runWatcher(watcher *fsnotify.Watcher) {
	defer watcher.Close()
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			err := handleEvent(&event)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to handle fsnotify event:\n%v\n", err.Error()))
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			logger.Info(fmt.Sprintf("Error received:\n%v", err.Error()))
		}
	}
}

// func handleNotifyEvent(event fsnotify.Event) (string, error) {
// }

func eventIs(op fsnotify.Op, event *fsnotify.Event) bool {
	return event.Op&op == op
}

func handleEvent(event *fsnotify.Event) error {
	var err error

	// get FileEvent
	fileEvent, err := toFileEvent(event)
	if err != nil {
		return err
	}
	if fileEvent == nil {
		return nil
	}

	// two types of create events | new files | moved/renamed files
	if fileEvent.Type == "create" {
		watchedInodeMap[fileEvent.Watched.Meta.Ino] = fileEvent.Watched
		watchedPathMap[fileEvent.State.Path] = fileEvent.Watched
	}

	watched := fileEvent.Watched

	// we don't add rename events to our file log until we get the corresponding create event
	// logger.Trace(fmt.Sprintln(watched, fileEvent.State))
	// logger.Trace(fmt.Sprintf("Appending event to log {id: %v, log: %v}{type: %v, path: %v}",
	// 	watched.UUID,
	// 	watched.Log,
	// 	fileEvent.Type,
	// 	fileEvent.State.Path,
	// ))
	watched.Latest = fileEvent
	watched.Log = append(watched.Log, fileEvent)
	watched.Latest = fileEvent

	// TODO: remove
	// for debug purposes
	if fileEvent.Type == "remove" {
		for i, e := range watched.Log {
			var path string
			if e.State != nil {
				path = e.State.Path
			}
			logger.Info(fmt.Sprintf("%d - %p - %v %q\n", i, e, e.Type, path))
		}
	}

	return err
}

// translate fsnotify.Event to local type incl. file hash
func toFileEvent(event *fsnotify.Event) (*FileEvent, error) {
	dirPath, dir := getEventManagedDir(event.Name)
	if dirPath == "" || dir == nil {
		return nil, errors.New(fmt.Sprintf("Failed to get managed dir for event: %v", event)) // if this ever happens something funky is probably going on
	}

	relPath := getRelativePath(event.Name, dirPath)

	fileEvent := &FileEvent{
		// Path:      relPath,
		Timestamp: time.Now(),
	}

	switch {
	case eventIs(fsnotify.Create, event):
		logger.Info(fmt.Sprintf("Create event: %q", relPath))

		fileEvent.Type = "create"

		info, err := os.Lstat(event.Name)
		if err != nil {
			return nil, err
		}

		meta, err := getNodeMeta(info)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to get file meta for %q\n%v", event.Name, err.Error()))
		}

		if meta == nil {
			return nil, nil
		}

		state, err := getNodeState(relPath, event.Name, info)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to get file state for %q\n%v", event.Name, err.Error()))
		}

		fileEvent.State = state

		watched, ok := watchedInodeMap[meta.Ino]
		// handle renames
		if ok {
			// swamp name in map
			watchedPathMap[fileEvent.State.Path] = watched
			delete(watchedPathMap, watched.Latest.State.Path)

			// update latest event w/ new info
			logger.Info(fmt.Sprintf("Rename event | %q | %+v\n", fileEvent.State.Path, watched.UUID))

			fileEvent.Type = "rename"
			fileEvent.Watched = watched

			// delete rename event from lookup
			delete(renameEvents, fileEvent.Watched.Meta.Ino)
		} else {
			fileEvent.Watched = &WatchedNode{
				UUID:   uuid.New(),
				Meta:   meta,
				Log:    []*FileEvent{},
				Latest: nil,
			}
		}

	case eventIs(fsnotify.Write, event):
		logger.Info(fmt.Sprintf("Write event: %q", relPath))

		fileEvent.Type = "write"

		info, err := os.Lstat(event.Name)
		if err != nil {
			return nil, err
		}

		meta, err := getNodeMeta(info)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to get file meta for %q\n%v", event.Name, err.Error()))
		}

		if meta == nil {
			return nil, nil
		}

		state, err := getNodeState(relPath, event.Name, info)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to get file state for %q\n%v", event.Name, err.Error()))
		}

		fileEvent.State = state

		watched, nodeExists := watchedInodeMap[meta.Ino]
		if !nodeExists {
			logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
			os.Exit(1)
		}
		if nodeExists {
			fileEvent.Watched = watched
		}

	case eventIs(fsnotify.Remove, event):
		logger.Info(fmt.Sprintf("Remove event: %q", relPath))
		fileEvent.Type = "remove"
		watched, nodeExists := watchedPathMap[relPath]
		if !nodeExists {
			logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
			os.Exit(1)
		}
		fileEvent.Watched = watched

	// case eventIs(fsnotify.Rename, event):
	// logger.Info(fmt.Sprintf("Rename event: %q", relPath))
	// fileEvent.Type = "rename-signal"
	// watched, nodeExists := watchedPathMap[relPath]
	// if !nodeExists {
	// 	logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
	// 	os.Exit(1)
	// }
	// fileEvent.Watched = watched

	default:
		fileEvent = nil
		logger.Trace(fmt.Sprintf("%v event: %q", event.Op, relPath))
	}

	return fileEvent, nil
}
func getEventManagedDir(name string) (string, *ManagedDirectory) {
	// TODO: at some point we need to disallow or handle nested managed directories
	for dirPath, managedDir := range managedDirMap {
		if strings.HasPrefix(name, dirPath) {
			return dirPath, &managedDir
		}
	}
	return "", nil
}
