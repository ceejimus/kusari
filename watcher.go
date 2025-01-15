package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

type NodeType int

const (
	FILE NodeType = iota + 1
	DIR
)

type NodeMeta struct {
	Type NodeType // file, dir, link ,etc
	Ino  uint64   // inode
}

// TODO: make "enum"
type FileEvent struct {
	Type      string       // "create", "modify", "delete", "rename"
	Timestamp time.Time    // time event processed
	Path      string       // path of event (current or prior)
	Hash      *string      // file hash
	Node      *WatchedNode // pointer to parent node
}

// TODO make sure this stays sorted
type WatchedNode struct {
	UUID   uuid.UUID    // current path
	Meta   NodeMeta     // node metadata
	Log    []*FileEvent // events for this file
	Latest *FileEvent   // pointer to most recent event
}

// TODO use DB instead of in-memory not thread-safe thing
type FileLog map[string]*WatchedNode

type RenameEvents map[uint64]*FileEvent // map nodes that got rename events

var fileLog = make(FileLog)
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
		if isRename := handleRename(fileEvent); !isRename {
			fileLog[fileEvent.Path] = fileEvent.Node
		}
	}

	// important to use THIS node since the fileEvent BEFORE handleRename may be a "pseudo" node
	node := fileEvent.Node

	// we don't add rename events to our file log until we get the corresponding create event
	if fileEvent.Type == "rename-signal" {
		renameEvents[fileEvent.Node.Meta.Ino] = fileEvent
	} else {
		logger.Trace(fmt.Sprintf("Appending event to log {id: %v, log: %v}{type: %v, path: %v}", node.UUID, node.Log, fileEvent.Type, fileEvent.Path))
		node.Latest = fileEvent
		node.Log = append(node.Log, fileEvent)
		node.Latest = fileEvent
	}

	// TODO: remove
	// for debug purposes
	if fileEvent.Type == "remove" {
		for i, e := range node.Log {
			logger.Info(fmt.Sprintf("%d - %p - %v %q\n", i, e, e.Type, e.Path))
		}
	}

	return err
}

func toFileEvent(event *fsnotify.Event) (*FileEvent, error) {
	var meta *NodeMeta
	dirPath, dir := getEventManagedDir(event.Name)
	if dirPath == "" || dir == nil {
		return nil, errors.New(fmt.Sprintf("Failed to get managed dir for event: %v", event)) // if this ever happens something funky is probably going on
	}

	relPath := getRelativePath(event.Name, dirPath)

	fileEvent := &FileEvent{
		Path:      relPath,
		Timestamp: time.Now(),
	}

	node, nodeExists := fileLog[relPath]
	if nodeExists {
		fileEvent.Node = node
		meta = &node.Meta
	}

	// translate fsnotify.Event to local type incl. file hash
	switch {
	case eventIs(fsnotify.Create, event):
		logger.Info(fmt.Sprintf("Create event: %q", relPath))

		fileEvent.Type = "create"

		logger.Trace(fmt.Sprintf("Meta is nil"))
		// this is a true new node not a rename/move
		meta, err := getNodeMeta(event.Name)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to get inode for %q\n%v", event.Name, err.Error()))
		}
		if meta == nil {
			return nil, nil
		}

		if meta.Type == FILE {
			hash, err := fileHash(event.Name)
			if err != nil {
				return nil, err
			}
			fileEvent.Hash = &hash
		}

		fileEvent.Node = &WatchedNode{
			UUID:   uuid.New(),
			Meta:   *meta,
			Log:    []*FileEvent{},
			Latest: nil,
		}

	case eventIs(fsnotify.Write, event):
		logger.Info(fmt.Sprintf("Write event: %q", relPath))

		fileEvent.Type = "write"

		if meta.Type == FILE {
			hash, err := fileHash(event.Name)
			if err != nil {
				return nil, err
			}
			fileEvent.Hash = &hash
		}

	case eventIs(fsnotify.Remove, event):
		logger.Info(fmt.Sprintf("Remove event: %q", relPath))
		fileEvent.Type = "remove"

	case eventIs(fsnotify.Rename, event):
		logger.Info(fmt.Sprintf("Rename event: %q", relPath))
		fileEvent.Type = "rename-signal"

	default:
		fileEvent = nil
		logger.Trace(fmt.Sprintf("%v event: %q", event.Op, relPath))
	}

	return fileEvent, nil
}

func handleRename(fileEvent *FileEvent) bool {
	// if we find a rename event via hash then this is the final event of a rename/create pair
	// to us a rename event is one event containing the new filename
	if renameEvent, ok := renameEvents[fileEvent.Node.Meta.Ino]; ok {
		// replace file history key w/ the new path
		fileLog[fileEvent.Path] = renameEvent.Node
		delete(fileLog, renameEvent.Path)

		// update latest event w/ new info
		logger.Info(fmt.Sprintf("Rename event | %q | %+v\n", fileEvent.Path, renameEvent.Node.UUID))
		renameEvent.Type = "rename"
		renameEvent.Hash = fileEvent.Hash
		renameEvent.Path = fileEvent.Path

		// delete rename event from lookup
		delete(renameEvents, fileEvent.Node.Meta.Ino)

		// our new event is the re-constructed rename event w/ new path
		*fileEvent = *renameEvent // IMPORTANT

		return true
	}

	return false
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

func getNodeMeta(path string) (*NodeMeta, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	logger.Trace(fmt.Sprintf("FileMode for %q | %v", path, info.Mode()))
	nodeType := getNodeType(info.Mode())

	if nodeType < 1 {
		// unsupported node
		return nil, nil
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.ErrUnsupported
	}

	return &NodeMeta{
		Type: nodeType,
		Ino:  stat.Ino,
	}, nil
}

func getNodeType(fileMode fs.FileMode) NodeType {
	if fileMode.IsRegular() {
		return FILE
	}

	if fileMode.IsDir() {
		return DIR
	}

	return -1
}
