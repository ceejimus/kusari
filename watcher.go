package main

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

// TODO: make "enum"
type FileEvent struct {
	Name      string    // the name of the underlying event (full path)
	Type      string    // "create", "modify", "delete", "rename"
	Timestamp time.Time // time event processed
	// NodeType  NodeType   // the type of node
	State *NodeState // node state
	Next  *FileEvent // next event for this node
	Prev  *FileEvent // previous event for this node
}

type WatchedNode struct {
	UUID uuid.UUID  // id
	Head *FileEvent // head of list
	Tail *FileEvent // pointer to most recent event
	Node *Node      // the node we're watching
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
		Timestamp: time.Now(),
		State:     &state,
		// Watched:   nil,
	}
	watched := WatchedNode{
		UUID: uuid.New(),
		Head: &fileEvent,
		Tail: &fileEvent,
		Node: node,
	}
	// fileEvent.Watched = &watched
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
			fileEvent, newDir, err := handleEvent(&event)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to handle fsnotify event:\n%v\n", err.Error()))
			}

			if fileEvent == nil {
				continue
			}

			if newDir {
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

func handleEvent(event *fsnotify.Event) (*FileEvent, bool, error) {
	// transform fsnotify event into local node event
	fileEvent := parseEvent(event)
	if fileEvent == nil {
		return nil, false, nil
	}
	// lookup watched log for this event
	watched, node, err := lkpWatched(fileEvent)
	if err != nil {
		return nil, false, errors.New(fmt.Sprintf("Failed to find Watched for event:  %v", event))
	}
	// ignore unsupported types for now
	if node.Type() < 1 {
		return nil, false, errors.ErrUnsupported
	}
	// link events together and create new WatchedNode if necessary
	newWatched := linkEvents(fileEvent, watched, node)
	if newWatched != nil {
		// TODO: make sure this works w/ new rename/recreate/reuse paradigm
		watchedInodeMap[newWatched.Node.Ino()] = newWatched
		watchedPathMap[newWatched.Node.Path] = newWatched
	}
	setNodeState(fileEvent, node)

	if fileEvent.Type == "remove" {
		for e := watched.Head; e != nil; e = e.Next {
			logger.Info(fmt.Sprintf("%p - %v %s\n", e, e.Type, e.State))
		}
	}

	return fileEvent, fileEvent.Type == "create" && node.Type() == DIR, nil
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

func setNodeState(fileEvent *FileEvent, node *Node) {
	// match event name (full path to file) to managed directory
	// dirPath, dir := getManagedDirFromFullPath(fileEvent.Name)
	// if dirPath == "" || dir == nil {
	// 	return nil, errors.New(fmt.Sprintf("Failed to get managed dir for event: %v", fileEvent)) // if this ever happens something funky is probably going on
	// }
	// get relative path of file
	// relPath := getRelativePath(fileEvent.Name, dirPath)

	switch fileEvent.Type {
	case "create":
		logger.Trace(fmt.Sprintf("Create event: %s", node.String()))
		// set fileEvent props
		nodeState := node.State()
		fileEvent.State = &nodeState
		// set node type
		// fileEvent.NodeType = node.Type()
	case "write":
		logger.Trace(fmt.Sprintf("Write event: %s", node.String()))
		// set fileEvent props
		nodeState := node.State()
		fileEvent.State = &nodeState
		// set node type
		// fileEvent.NodeType = node.Type()
	default:
	}
}

func linkEvents(fileEvent *FileEvent, watched *WatchedNode, node *Node) *WatchedNode {
	if fileEvent.Type == "create" {
		// REMEMBER: we find this watched by inode lookup
		if watched != nil { // this is either a rename, a recreate, or a reuse of inode
			if watched.Tail.Type == "rename" { // rename
				// swamp name in map // shrek is love
				watchedPathMap[fileEvent.Name] = watched
				delete(watchedPathMap, watched.Tail.Name)
			} else if watched.Tail.Type != "remove" || watched.Tail.Name != fileEvent.Name { // reuse of inode
				return &WatchedNode{
					UUID: uuid.New(),
					Head: fileEvent,
					Tail: fileEvent,
					Node: node,
				}
			}
		} else {
			return &WatchedNode{
				UUID: uuid.New(),
				Head: fileEvent,
				Tail: fileEvent,
				Node: node,
			}
		}
	}

	watched.Tail.Next = fileEvent // tail is now this event
	fileEvent.Prev = watched.Tail // event prior to this on was previous tail
	watched.Tail = fileEvent

	if node != nil {
		watched.Node = node
	}

	return nil
}

// func
// 	switch fileEvent.Type {
// 	case "create":
// 		logger.Trace(fmt.Sprintf("Create event: %s", node.String()))
// 		// set fileEvent props
// 		nodeState := node.State()
// 		fileEvent.State = &nodeState
// 		// set node type
// 		fileEvent.NodeType = node.Type()
// 	case "write":
// 		logger.Trace(fmt.Sprintf("Write event: %s", node.String()))
// 		// set fileEvent props
// 		nodeState := node.State()
// 		fileEvent.State = &nodeState
// 		// set node type
// 		fileEvent.NodeType = node.Type()
// 	default:
// 	}
// 	// do the magic sauce
// 	switch {
// 	// fsnotify.Create events occur for new inodes and renamed inodes
// 	// for renamed inodes the create event occurs AFTER a rename event
// 	// since we're tracking at the inode level the rename event is kinda useless
// 	case eventIs(fsnotify.Create, fileEvent):
//
// 	// write events occur when you modify the contents of a file
// 	// writes may occur one after another // TODO: handle write event "streams"
// 	case eventIs(fsnotify.Write, fileEvent):
// 		// we should be watching this node
// 		logger.Trace(fmt.Sprintf("inode: %d", node.Ino()))
// 		watched, nodeExists := watchedInodeMap[node.Ino()]
// 		if !nodeExists {
// 			logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
// 			os.Exit(1)
// 		}
//
// 		watched.Node = node
// 	// fsnotify.Remove events occur on (you guessed it)
// 	case eventIs(fsnotify.Remove, fileEvent):
// 		logger.Trace(fmt.Sprintf("Remove event: %q", relPath))
// 		// we should be watching this node
// 		watched, nodeExists := watchedPathMap[fileEvent.Name]
// 		if !nodeExists {
// 			logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
// 			os.Exit(1)
// 		}
// 		// set fileEvent props
// 		fileEvent.Type = "remove"
// 		fileEvent.NodeType = watched.Node.Type()
// 	case eventIs(fsnotify.Rename, fileEvent):
// 		logger.Info(fmt.Sprintf("Rename event: %q", relPath))
// 		watched, nodeExists := watchedPathMap[fileEvent.Name]
// 		if !nodeExists {
// 			logger.Error(fmt.Sprintf("Can't find watched for %q", relPath))
// 			os.Exit(1)
// 		}
// 		// set fileEvent props
// 		fileEvent.Type = "rename"
// 		fileEvent.NodeType = watched.Node.Type()
// 	// nil the fileEvent pointer and trace it for unhandled events
// 	default:
// 		logger.Trace(fmt.Sprintf("%v event: %q", fileEvent.Op, relPath))
// 		fileEvent = nil
// 	}
//
// 	return fileEvent, nil
// }

func eventIs(op fsnotify.Op, event *fsnotify.Event) bool {
	return event.Op&op == op
}

func lkpWatched(fileEvent *FileEvent) (*WatchedNode, *Node, error) {
	switch fileEvent.Type {
	case "create":
		node, err := newNode(fileEvent.Name)
		if err != nil {
			return nil, node, err
		}
		return watchedInodeMap[node.Ino()], node, nil
	case "write":
		node, err := newNode(fileEvent.Name)
		if err != nil {
			return nil, node, err
		}
		return watchedInodeMap[node.Ino()], node, nil
	case "remove":
		watched := watchedPathMap[fileEvent.Name]
		return watched, watched.Node, nil
	case "rename":
		watched := watchedPathMap[fileEvent.Name]
		return watched, watched.Node, nil
	default:
		return nil, nil, errors.ErrUnsupported
	}
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
