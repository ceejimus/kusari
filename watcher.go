package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

// TODO: make "enum"
type FileEvent struct {
	Name      string       // the name of the underlying event (full path)
	Type      string       // "create", "modify", "delete", "rename"
	Timestamp time.Time    // time event processed
	NodeType  NodeType     // the type of node
	State     *NodeState   // node state
	Watched   *WatchedNode // pointer to parent watched node // TODO: remove
}

type WatchedNode struct {
	UUID   uuid.UUID    // id
	Log    []*FileEvent // events for this file
	Latest *FileEvent   // pointer to most recent event
	Node   *Node        // node metadata
}

// for create / write we can (and should) find watched via inode
type WatchedInodeMap map[uint64]*WatchedNode

// for rename / remove we don't have an inode anymore and can find by path
type WatchedPathMap map[string]*WatchedNode

var watchedNodes []WatchedNode
var watchedInodeMap WatchedInodeMap
var watchedPathMap WatchedPathMap
var managedDirMap map[string]ManagedDirectory

func initWatcher(config *NodeConfig, managedMap ManagedMap) *fsnotify.Watcher {
	// init globals
	watchedNodes = make([]WatchedNode, 0)
	watchedInodeMap = make(WatchedInodeMap)
	watchedPathMap = make(WatchedPathMap)
	managedDirMap = make(map[string]ManagedDirectory)
	// create a new watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error(err.Error())
		return nil
	}
	// add managed dirs to watcher
	for _, dir := range config.ManagedDirectories {
		// TODO: ignore events based on globs
		dirPath := filepath.Join(config.TopDir, dir.Path)

		nodes := managedMap[dir.Path]
		// populate event logs w/ pseudos based on current state
		for _, node := range nodes {
			watched := pseudoWatched(&node)
			watchedInodeMap[node.Ino()] = watched
			watchedPathMap[node.Path] = watched
		}

		managedDirMap[dirPath] = dir // TODO: does this copy underlying array, maybe don't worry about it

		err := recursiveWatcherAdd(watcher, dirPath)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", dirPath))
			return nil
		}
		logger.Info(fmt.Sprintf("Watching %q\n", dirPath))
	}

	return watcher
}

func pseudoWatched(node *Node) *WatchedNode {
	state := node.State()
	fileEvent := FileEvent{
		Name:      node.Path,
		Type:      "create",
		NodeType:  node.Type(),
		Timestamp: time.Now(),
		State:     &state,
		Watched:   nil,
	}
	watched := WatchedNode{
		UUID:   uuid.New(),
		Log:    []*FileEvent{&fileEvent},
		Latest: &fileEvent,
		Node:   node,
	}
	fileEvent.Watched = &watched
	return &watched
}

func runWatcher(watcher *fsnotify.Watcher) {
	defer watcher.Close()
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			fileEvent, err := handleEvent(&event)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to handle fsnotify event:\n%v\n", err.Error()))
			}

			if fileEvent == nil {
				continue
			}

			if fileEvent.Type == "create" && fileEvent.NodeType == DIR {
				logger.Trace(fmt.Sprintf("Watching new dir: %q", fileEvent.Name))
				err := recursiveWatcherAdd(watcher, fileEvent.Name)
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", fileEvent.Name))
				}
			}
		case err, ok := <-watcher.Errors:
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

func handleEvent(event *fsnotify.Event) (*FileEvent, error) {
	// transform fsnotify event into local node event
	fileEvent, err := toFileEvent(event)
	if err != nil {
		return nil, err
	}
	if fileEvent == nil {
		return nil, nil
	}
	// add new watched nodes to maps
	if fileEvent.Type == "create" {
		// TODO: make sure this works w/ new rename/recreate/reuse paradigm
		watchedInodeMap[fileEvent.Watched.Node.Ino()] = fileEvent.Watched
		watchedPathMap[fileEvent.Name] = fileEvent.Watched
	}
	// update watched file event log
	watched := fileEvent.Watched
	watched.Latest = fileEvent
	watched.Log = append(watched.Log, fileEvent)
	watched.Latest = fileEvent
	// TODO: remove
	// for debug purposes
	if fileEvent.Type == "remove" {
		for i, e := range watched.Log {
			logger.Info(fmt.Sprintf("%d - %p - %v %q\n", i, e, e.Type, e.Name))
		}
	}

	return fileEvent, nil
}

// translate fsnotify.Event to local type incl. file hash
func toFileEvent(event *fsnotify.Event) (*FileEvent, error) {
	// match event name (full path to file) to managed directory
	dirPath, dir := getManagedDirFromFullPath(event.Name)
	if dirPath == "" || dir == nil {
		return nil, errors.New(fmt.Sprintf("Failed to get managed dir for event: %v", event)) // if this ever happens something funky is probably going on
	}
	// get relative path of file
	relPath := getRelativePath(event.Name, dirPath)
	// init fileEvent
	fileEvent := &FileEvent{
		Name:      event.Name,
		Timestamp: time.Now(),
	}
	// do the magic sauce
	switch {
	// fsnotify.Create events occur for new inodes and renamed inodes
	// for renamed inodes the create event occurs AFTER a rename event
	// since we're tracking at the inode level the rename event is kinda useless
	case eventIs(fsnotify.Create, event):
		logger.Trace(fmt.Sprintf("Create event: %q", relPath))
		// make new node
		node, err := newNode(event.Name)
		if err != nil {
			return nil, err
		}
		// ignore unsupported types for now
		if node.Type() < 1 {
			return nil, nil
		}
		// set fileEvent props
		fileEvent.Type = "create"
		fileEvent.NodeType = node.Type()
		nodeState := node.State()
		fileEvent.State = &nodeState
		// are we already watching this node?
		logger.Trace(fmt.Sprintf("inode: %d", node.Ino()))
		watched, ok := watchedInodeMap[node.Ino()]
		if ok { // this is either a rename | recreate | reuse (ino)
			if watched.Latest.Type == "rename" { // rename
				// swamp name in map
				// shrek is love
				watchedPathMap[fileEvent.Name] = watched
				delete(watchedPathMap, watched.Latest.Name)
				// update latest event w/ new info
				logger.Info(fmt.Sprintf("Rename event | %q | %+v\n", fileEvent.State.Path, watched.UUID))
			}
			watched.Node = node
			fileEvent.Watched = watched
		} else { // then this is a new inode
			fileEvent.Watched = &WatchedNode{
				UUID:   uuid.New(),
				Node:   node,
				Log:    []*FileEvent{}, // this file event will get added by caller
				Latest: nil,
			}
		}
	// write events occur when you modify the contents of a file
	// writes may occur one after another // TODO: handle write event "streams"
	case eventIs(fsnotify.Write, event):
		logger.Trace(fmt.Sprintf("Write event: %q", relPath))
		// make new node
		node, err := newNode(event.Name)
		// ignore unsupported types for now
		if err != nil {
			return nil, err
		}
		// set fileEvent props
		fileEvent.Type = "write"
		fileEvent.NodeType = node.Type()
		nodeState := node.State()
		fileEvent.State = &nodeState
		// we should be watching this node
		logger.Trace(fmt.Sprintf("inode: %d", node.Ino()))
		watched, nodeExists := watchedInodeMap[node.Ino()]
		if !nodeExists {
			logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
			os.Exit(1)
		}

		watched.Node = node
		fileEvent.Watched = watched
	// fsnotify.Remove events occur on (you guessed it)
	case eventIs(fsnotify.Remove, event):
		logger.Trace(fmt.Sprintf("Remove event: %q", relPath))
		// we should be watching this node
		watched, nodeExists := watchedPathMap[event.Name]
		if !nodeExists {
			logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
			os.Exit(1)
		}
		// set fileEvent props
		fileEvent.Type = "remove"
		fileEvent.NodeType = watched.Node.Type()
		fileEvent.Watched = watched
	case eventIs(fsnotify.Rename, event):
		logger.Info(fmt.Sprintf("Rename event: %q", relPath))
		watched, nodeExists := watchedPathMap[event.Name]
		if !nodeExists {
			logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
			os.Exit(1)
		}
		// set fileEvent props
		fileEvent.Type = "rename"
		fileEvent.NodeType = watched.Node.Type()
		fileEvent.Watched = watched
	// nil the fileEvent pointer and trace it for unhandled events
	default:
		logger.Trace(fmt.Sprintf("%v event: %q", event.Op, relPath))
		fileEvent = nil
	}

	return fileEvent, nil
}

func eventIs(op fsnotify.Op, event *fsnotify.Event) bool {
	return event.Op&op == op
}

func getManagedDirFromFullPath(name string) (string, *ManagedDirectory) {
	// TODO: at some point we need to disallow or handle nested managed directories
	for dirPath, managedDir := range managedDirMap {
		if strings.HasPrefix(name, dirPath) {
			return dirPath, &managedDir
		}
	}
	return "", nil
}
