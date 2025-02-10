package syncd

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"atmoscape.net/fileserver/fnode"
	"atmoscape.net/fileserver/logger"
	"atmoscape.net/fileserver/utils"
	"github.com/fsnotify/fsnotify"
)

type Watcher struct {
	ProcessedChanRx <-chan NodeEvent
	inner           *fsnotify.Watcher
	store           EventStore
	topDir          string
	dirPaths        []string
	processedChanTx chan<- NodeEvent
	stopmu          sync.Mutex
	stopping        int32
	stopped         int32
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

func (w *Watcher) Stop() {
	w.stopmu.Lock()
	defer w.stopmu.Unlock()
	if atomic.LoadInt32(&w.stopping) == 1 {
		return
	}
	for _, d := range w.dirPaths {
		recursiveWatcherRemove(w.inner, filepath.Join(w.topDir, d))
	}
	atomic.StoreInt32(&w.stopping, 1)
}

func (w *Watcher) Close() error {
	w.Stop()
	for atomic.LoadInt32(&w.stopped) == 0 {
		time.Sleep(time.Millisecond * 50)
	}
	return w.inner.Close()
}

func (w *Watcher) Run() {
	inner := w.inner
	// run loop
	for {
		select {
		case event, ok := <-inner.Events:
			if !ok { // channel closed
				return
			}
			// process the event
			logger.Trace(fmt.Sprintf("Received watcher event %s", event))

			nodeEvent, err := processEvent(w, event)
			if err != nil {
				logger.Error(err.Error())
				continue
			}
			// update watcher for new dirs
			updateWatcher(w, nodeEvent)
		case err, ok := <-inner.Errors:
			if !ok { // channel closed
				return
			}
			logger.Error(fmt.Sprintf("Received watcher error:\n%v", err.Error()))
		default: // no events from fsnotify
			// if we're stopping and we've no more events
			if atomic.LoadInt32(&w.stopping) == 1 {
				logger.Info(fmt.Sprintf("No more events in fsnotify.Watcher. We're done!"))
				atomic.StoreInt32(&w.stopped, 1)
				return
			}
		}
	}
}

func InitWatcher(topDir string, syncdDirs []SyncdDirectory, store EventStore) (*Watcher, error) {
	// create a watcher
	inner, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	// get stored dirs
	dirs, err := store.GetDirs()

	if err != nil {
		return nil, err
	}

	// create watcher
	tx, rx := utils.NewDroppingChannel[NodeEvent](1024)
	w := Watcher{
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
		w.dirPaths[i] = dir.Path
		// recursively add watcher to the directory and all sub-directories
		if err = recursiveWatcherAdd(&w, filepath.Join(topDir, dir.Path)); err != nil {
			return nil, err
		}
	}

	return &w, nil
}

func processEvent(watcher *Watcher, event fsnotify.Event) (*NodeEvent, error) {
	// transform fsnotify event into local node event
	nodeEvent := toNodeEvent(&event)
	if nodeEvent == nil { // ignored event
		return nil, nil
	}
	// get relative paths for store
	relPath := fnode.GetRelativePath(nodeEvent.FullPath, watcher.topDir)
	// lookup stored directory by event name
	dir := watcher.getDirForEvent(relPath)
	if dir == nil {
		return nodeEvent, errors.New(fmt.Sprintf("Failed to find dir for event: %s", event))
	}
	nodeEvent.dir = dir
	// get relative path to node
	nodeEvent.Path = fnode.GetRelativePath(relPath, dir.Path)
	// process this event
	if err := processNodeEvent(nodeEvent, watcher.store); err != nil {
		logger.Error(fmt.Sprintf("Failed to handle fsnotify event:\n%v\n", err.Error()))
	}
	// send processed event out
	nodeEvent.doneTime = time.Now()
	watcher.processedChanTx <- *nodeEvent
	return nodeEvent, nil
}

func updateWatcher(w *Watcher, nodeEvent *NodeEvent) {
	w.stopmu.Lock()
	defer w.stopmu.Unlock()
	if atomic.LoadInt32(&w.stopping) == 1 {
		return
	}
	// add/remove sub-directories to watcher
	// no need to check if node is dir since the function won't walk if it is
	if nodeEvent.Type == Create {
		if err := recursiveWatcherAdd(w, nodeEvent.FullPath); err != nil {
			logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", nodeEvent.FullPath))
		}
		// if this ISN'T a moved directory
		if nodeEvent.OldPath == nil {
			if err := addSubDirsToStore(w, nodeEvent); err != nil {
				logger.Error(fmt.Sprintf("Failed to add %q to watcher.\n", nodeEvent.FullPath))
			}
		}
	} else if nodeEvent.Type == Rename || nodeEvent.Type == Remove {
		recursiveWatcherRemove(w.inner, nodeEvent.FullPath)
	}
}

// add directory and all sub-directories (recursively) to watcher
// adding a directory to an fsnotify watcher will track events for subdirs themselves,
// but not nodes inside those subdirs - see fsnotify docs
// also ensure any nodes w/o chains have chains created w/ "artificial" create event
func recursiveWatcherAdd(w *Watcher, newPath string) error {
	// recursively walk directory - starting at top
	err := filepath.WalkDir(newPath, func(path string, d fs.DirEntry, err error) error {
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
		w.inner.Add(path)
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

// this function adds new chains and create events for sub-dirs of watched dirs
// this covers the case when a new subdirectory w/ content is made before
// we've added the subdirectory to the watch list (e.g. mkdir -p)
func addSubDirsToStore(w *Watcher, nodeEvent *NodeEvent) error {
	syncdDirPath := filepath.Join(w.topDir, nodeEvent.dir.Path)
	dirPath := filepath.Join(syncdDirPath, nodeEvent.Path)
	return filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		// skil the root dir of walk as we've already added event
		if path == dirPath {
			return nil
		}
		// an inability to walk a sub-directory would cause problems, but wouldn't be fatal
		if err != nil {
			logger.Warn(fmt.Sprintf("Unable to walk dir: %q", path))
			return nil
		}
		// we only want to watch directories and normal files
		if !d.Type().IsRegular() && !d.Type().IsDir() {
			logger.Trace(fmt.Sprintf("SKIPPING - %v : %v", path, d))
			return nil
		}

		eventPath := fnode.GetRelativePath(path, syncdDirPath)
		filepath.Join(dirPath, d.Name())
		// create node from DirEntry
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		node, err := fnode.NewNodeFromInfo(path, info)
		if err != nil {
			return err
		}
		// add chain for new node
		chain := &Chain{Ino: node.Ino}
		// add new chain
		if err = w.store.AddChain(chain, nodeEvent.dir.ID); err != nil {
			logger.Fatal(err.Error())
			os.Exit(1)
		}
		// add create event to chain
		state := node.State()
		event := Event{
			Timestamp: time.Now(),
			Path:      eventPath,
			Type:      Create,
			Size:      state.Size,
			Hash:      state.Hash,
			ModTime:   state.ModTime,
		}
		if err = w.store.AddEvent(&event, chain.ID); err != nil {
			return err
		}
		return nil
	})
}
