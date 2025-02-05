package syncd

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"atmoscape.net/fileserver/fnode"
	"atmoscape.net/fileserver/logger"
	"atmoscape.net/fileserver/utils"
	"github.com/fsnotify/fsnotify"
)

type Watcher struct {
	inner           *fsnotify.Watcher
	store           EventStore
	topDir          string
	dirPaths        []string
	processedChanTx chan<- NodeEvent
	ProcessedChanRx <-chan NodeEvent
}

// TODO: make "enum"
type NodeEvent struct {
	Type      EventType        // "create", "modify", "delete", "rename"
	FullPath  string           // the name of the underlying event (full path)
	Path      string           // path of node relative to Dir.Path
	Timestamp time.Time        // time event processed
	State     *fnode.NodeState // node state pointer
	node      *fnode.Node      // node pointer
	dir       *Dir             // stored Dir for this event
	chain     *Chain           // stored Chain for this event
}

func (t EventType) String() string {
	switch t {
	case Chmod:
		return "chmod"
	case Create:
		return "create"
	case Remove:
		return "remove"
	case Rename:
		return "rename"
	case Write:
		return "write"
	default:
		panic(fmt.Sprintf("unexpected syncd.NodeEventType: %#v", t))
	}
}

func (w *Watcher) getDirForEvent(nodePath string) *Dir {
	for _, dirPath := range w.dirPaths {
		if strings.HasPrefix(nodePath, dirPath) {
			dir, _ := w.store.GetDirByPath(dirPath)
			return dir
		}
	}
	return nil
}

func InitWatcher(topDir string, managedDirs []ManagedDirectory, managedMap ManagedMap, store EventStore) (*Watcher, error) {
	// create a watcher
	inner, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	// get stored dirs
	dirs := store.GetDirs()

	// create watcher
	tx, rx := utils.NewDroppingChannel[NodeEvent](1024)
	watcher := Watcher{
		inner:           inner,
		store:           store,
		topDir:          topDir,
		dirPaths:        make([]string, len(dirs)),
		processedChanTx: tx,
		ProcessedChanRx: rx,
	}

	// add dirs to watcher
	for i, dir := range dirs {
		// add this path to paths for directory lookups on node event
		watcher.dirPaths[i] = dir.Path
		// recursively add watcher to the directory and all sub-directories
		if err = recursiveWatcherAdd(inner, filepath.Join(topDir, dir.Path)); err != nil {
			return nil, err
		}
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
			logger.Trace(fmt.Sprintf("Received watcher event %s", event))
			// transform fsnotify event into local node event
			nodeEvent := toNodeEvent(&event)
			if nodeEvent == nil { // ignored event
				continue
			}
			// get relative paths for store
			relPath := fnode.GetRelativePath(nodeEvent.FullPath, watcher.topDir)
			// lookup stored directory by event name
			dir := watcher.getDirForEvent(relPath)
			if dir == nil {
				logger.Error(fmt.Sprintf("Failed to find dir for event: %s", event))
				continue
			}
			nodeEvent.dir = dir
			// get relative path to node
			nodeEvent.Path = fnode.GetRelativePath(relPath, dir.Path)

			// handle this event
			if err := handleEvent(nodeEvent, watcher.store); err != nil {
				logger.Error(fmt.Sprintf("Failed to handle fsnotify event:\n%v\n", err.Error()))
			}
			watcher.processedChanTx <- *nodeEvent
			// add/remove sub-directories to watcher
			// no need to check if node is dir since the function won't walk if it is
			if nodeEvent.Type == Create {
				if err := recursiveWatcherAdd(inner, nodeEvent.FullPath); err != nil {
					logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", nodeEvent.FullPath))
				}
			} else if nodeEvent.Type == Rename || nodeEvent.Type == Remove {
				recursiveWatcherRemove(inner, nodeEvent.FullPath)
			}
		case err, ok := <-inner.Errors:
			if !ok { // channel closed
				return
			}
			logger.Error(fmt.Sprintf("Received watcher error:\n%v", err.Error()))
		}
	}
}

// add directory and all sub-directories (recursively) to watcher
// adding a directory to an fsnotify watcher will track events for subdirs themselves,
// but not nodes inside those subdirs - see fsnotify docs
func recursiveWatcherAdd(watcher *fsnotify.Watcher, path string) error {
	// recursively walk directory - starting at top
	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		// an inability to walk a sub-directory would cause problems, but wouldn't be fatal
		if err != nil {
			logger.Warn(fmt.Sprintf("Unable to walk dir: %q", path))
			return nil
		}
		// we only want to watch directories
		if !d.Type().IsDir() {
			return nil
		}
		// add path to watcher
		watcher.Add(path)
		logger.Debug(fmt.Sprintf("Watching %q\n", path))

		return nil
	})

	return err
}

// remove directory and all sub-directories (recursively) from watcher
// we can't walk a directory that's gone we just remove all watched paths by prefix
func recursiveWatcherRemove(watcher *fsnotify.Watcher, path string) {
	for _, watchedPath := range watcher.WatchList() {
		if strings.HasPrefix(watchedPath, path) {
			watcher.Remove(watchedPath)
		}
	}
}

func handleEvent(nodeEvent *NodeEvent, store EventStore) error {
	var err error

	// set node info on event
	if err = setNode(nodeEvent); err != nil {
		return errors.New(fmt.Sprintf("Failed to set node for: %+v", *nodeEvent))
	}
	// lookup chain log for this event
	if err = lkpChain(nodeEvent, store); err != nil {
		return errors.New(fmt.Sprintf("Failed to find lookup Chain for event:  %v", nodeEvent))
	}
	// ignore invalid events for now
	if err := isValidEvent(nodeEvent); err != nil {
		return err
	}
	// add new chain for new nodes
	if nodeEvent.Type == Create && nodeEvent.chain == nil {
		// create and new chain
		if nodeEvent.chain, err = store.AddChain(
			Chain{Ino: nodeEvent.node.Ino()},
			nodeEvent.dir.ID,
		); err != nil {
			logger.Fatal(err.Error())
			os.Exit(1)
		}
	}
	// create new event to store
	event := Event{
		Timestamp: nodeEvent.Timestamp,
		Path:      nodeEvent.Path,
		Type:      nodeEvent.Type,
	}
	// set event state from node
	setEventState(&event, nodeEvent.node)
	// add event to store
	if _, err = store.AddEvent(event, nodeEvent.chain.ID); err != nil {
		logger.Fatal(err.Error())
		os.Exit(1)
	}
	return nil
}

// toNodeEvent constructs a NodeEvent from an fsnotify.Event
// it add a timestamp and returns nil if this isn't an event we care about
func toNodeEvent(event *fsnotify.Event) *NodeEvent {
	// init nodeEvent
	nodeEvent := NodeEvent{
		FullPath:  event.Name,
		Timestamp: time.Now(),
	}
	// set internal type
	switch event.Op {
	case fsnotify.Create:
		nodeEvent.Type = Create
	case fsnotify.Write:
		nodeEvent.Type = Write
	case fsnotify.Remove:
		nodeEvent.Type = Remove
	case fsnotify.Rename:
		nodeEvent.Type = Rename
	default:
		return nil
	}

	return &nodeEvent
}

// setNode sets the node property on the NodeEvent struct
// the node is used to: detect new dirs, lookup chains by ino, set event state
func setNode(nodeEvent *NodeEvent) error {
	switch nodeEvent.Type {
	case Create, Write:
		node, err := fnode.NewNode(nodeEvent.FullPath)
		if err != nil {
			return err
		}
		nodeEvent.node = node
	case Rename, Remove:
	default:
		return errors.ErrUnsupported
	}

	return nil
}

// lkpChain finds a chain in the event store for a given event
// it uses inode lookup for creates/writes and paths for rename/remove
// in order to perform the inode lookup, lkpChain must go to the filesystem anyways
// so this function is also responsible for retrieving and returning node state
func lkpChain(nodeEvent *NodeEvent, store EventStore) error {
	var chain *Chain

	switch nodeEvent.Type {
	case Create, Write:
		chain, _ = store.GetChainByIno(nodeEvent.node.Ino())
	case Remove, Rename:
		chain, _ = store.GetChainByPath(
			nodeEvent.dir.ID,
			nodeEvent.Path,
		)
	default:
		return errors.ErrUnsupported
	}
	nodeEvent.chain = chain
	return nil
}

// isValidEvent ensures dirEvent is properly initialized for processing
func isValidEvent(nodeEvent *NodeEvent) error {
	node := nodeEvent.node
	chain := nodeEvent.chain
	switch nodeEvent.Type {
	case Create:
		if node == nil {
			return errors.New("create w/ no node")
		}
		if node.Type() < 1 {
			return errors.New(fmt.Sprintf("create unsupported node: %v", node))
		}
	case Write:
		if node == nil {
			return errors.New("write w/ no node")
		}
		if node.Type() < 1 {
			return errors.New(fmt.Sprintf("write unsupported node: %v", node))
		}
		if chain == nil {
			return errors.New(fmt.Sprintf("write w/ no chain"))
		}
	case Rename:
		if chain == nil {
			return errors.New(fmt.Sprintf("rename w/ no chain"))
		}
	case Remove:
		if chain == nil {
			return errors.New(fmt.Sprintf("remove w/ no chain"))
		}
	default:
		return errors.New(fmt.Sprintf("Unsupported event operation: %v", nodeEvent.Type))
	}
	return nil
}

func setEventState(event *Event, node *fnode.Node) {
	switch event.Type {
	case Create:
		// set event node state props
		nodeState := node.State()
		event.ModTime = node.ModTime()
		event.Size = node.Size()
		event.Hash = nodeState.Hash
	case Write:
		// set event node state props
		nodeState := node.State()
		event.ModTime = node.ModTime()
		event.Size = node.Size()
		event.Hash = nodeState.Hash
	default:
	}
}
