package test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"atmoscape.net/fileserver/badgerstore"
	"atmoscape.net/fileserver/fnode"
	"atmoscape.net/fileserver/logger"
	"atmoscape.net/fileserver/syncd"
	"atmoscape.net/fileserver/utils"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

type Chain []syncd.Event                                 // a chain of events
type Chains []Chain                                      // a slice of chains expected (given a final path)
type TailPathToChainMap map[string]Chains                // a map of final paths to expected chains
type DirPathToTailChainMap map[string]TailPathToChainMap // a map of dir names to path -> chains lookup

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	// shutdown()
	os.Exit(code)
}

var DIR_BLOCK_SIZE uint64

func setup() {
	logger.Init("")
	wd, err := os.Getwd()
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to get wd %s", err))
		os.Exit(-1)
	}
	var statfs unix.Statfs_t
	err = unix.Statfs(wd, &statfs)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to stat wd %q: %s", wd, err))
		os.Exit(-1)
	}
	DIR_BLOCK_SIZE = uint64(statfs.Bsize)
}

// test a remove for a single file
func TestExistingFileRemove(t *testing.T) {
	content := []byte("i am a")
	hash, err := fnode.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
		Files: []*utils.TmpFile{
			{Name: "a", Content: content},
		},
	}

	tmpFs := utils.TmpFs{Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.REMOVE, DstPath: "d/a"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test a create, write, rename, remove flow for single file
func TestSingleFileCWRR(t *testing.T) {
	content := []byte("I am Weasel!")
	hash, err := fnode.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["b"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Write,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "a",
				Type: syncd.Rename,
			},
			{
				Path: "b",
				Type: syncd.Create,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "b",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.WRITE, Content: content, DstPath: "d/a"},
		{Kind: utils.MOVE, SrcPath: "d/a", DstPath: "d/b"},
		{Kind: utils.REMOVE, DstPath: "d/b"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test creating a file w/ same old path as renamed file
func TestTouchMovedFilename(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["b"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Rename,
			},
			{
				Path: "b",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "b",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.MOVE, SrcPath: "d/a", DstPath: "d/b"},
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/b"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test moving files between subdirs
func TestFilesMovedBetweenSubdirs(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2"] = Chains{
		Chain{
			{
				Path: "s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/a"] = Chains{
		Chain{
			{
				Path: "s1/a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "s1/a",
				Type: syncd.Rename,
			},
			{
				Path: "s2/a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "s2/a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1"},
		{Kind: utils.MKDIR, DstPath: "d/s2"},
		{Kind: utils.TOUCH, DstPath: "d/s1/a"},
		{Kind: utils.MOVE, SrcPath: "d/s1/a", DstPath: "d/s2/a"},
		{Kind: utils.REMOVE, DstPath: "d/s2/a"},
		{Kind: utils.RMDIR, DstPath: "d/s1"},
		{Kind: utils.RMDIR, DstPath: "d/s2"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test moving/renaming a subdir w/ files
func TestEmptyDirsMovedBetweenSubdirs(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2"] = Chains{
		Chain{
			{
				Path: "s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s3"] = Chains{
		Chain{
			{
				Path: "s1/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1/s3",
				Type: syncd.Rename,
			},
			{
				Path: "s2/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2/s3",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1"},
		{Kind: utils.MKDIR, DstPath: "d/s2"},
		{Kind: utils.MKDIR, DstPath: "d/s1/s3"},
		{Kind: utils.MOVE, SrcPath: "d/s1/s3", DstPath: "d/s2/s3"},
		{Kind: utils.RMDIR, DstPath: "d/s2/s3"},
		{Kind: utils.RMDIR, DstPath: "d/s1"},
		{Kind: utils.RMDIR, DstPath: "d/s2"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test moving/renaming a subdir w/ files
func TestNonEmptyDirsMovedBetweenSubdirs(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2"] = Chains{
		Chain{
			{
				Path: "s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s3"] = Chains{
		Chain{
			{
				Path: "s1/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1/s3",
				Type: syncd.Rename,
			},
			{
				Path: "s2/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2/s3",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s3/a"] = Chains{
		Chain{
			{
				Path: "s1/s3/a",
				Type: syncd.Create,
			},
			{
				Path: "s2/s3/a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1"},
		{Kind: utils.MKDIR, DstPath: "d/s2"},
		{Kind: utils.MKDIR, DstPath: "d/s1/s3"},
		{Kind: utils.TOUCH, DstPath: "d/s1/s3/a"},
		{Kind: utils.MOVE, SrcPath: "d/s1/s3", DstPath: "d/s2/s3"},
		{Kind: utils.REMOVE, DstPath: "d/s2/s3/a"},
		{Kind: utils.RMDIR, DstPath: "d/s2/s3"},
		{Kind: utils.RMDIR, DstPath: "d/s1"},
		{Kind: utils.RMDIR, DstPath: "d/s2"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test moving/renaming a subdir w/ files
func TestMoveNonEmptySubdirThenMoveFileOut(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2"] = Chains{
		Chain{
			{
				Path: "s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s4"] = Chains{
		Chain{
			{
				Path: "s2/s4",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2/s4",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s4/s3"] = Chains{
		Chain{
			{
				Path: "s1/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1/s3",
				Type: syncd.Rename,
			},
			{
				Path: "s2/s4/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2/s4/s3",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s1/a"] = Chains{
		Chain{
			{
				Path: "s1/s3/a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "s2/s4/s3/a",
				Type: syncd.Rename,
			},
			{
				Path: "s1/a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "s1/a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1"},
		{Kind: utils.MKDIR, DstPath: "d/s2"},
		{Kind: utils.MKDIR, DstPath: "d/s1/s3"},
		{Kind: utils.TOUCH, DstPath: "d/s1/s3/a"},
		{Kind: utils.MKDIR, DstPath: "d/s2/s4"},
		{Kind: utils.MOVE, SrcPath: "d/s1/s3", DstPath: "d/s2/s4/s3"},
		{Kind: utils.MOVE, SrcPath: "d/s2/s4/s3/a", DstPath: "d/s1/a"},
		{Kind: utils.REMOVE, DstPath: "d/s1/a"},
		{Kind: utils.RMDIR, DstPath: "d/s2/s4/s3"},
		{Kind: utils.RMDIR, DstPath: "d/s2/s4"},
		{Kind: utils.RMDIR, DstPath: "d/s2"},
		{Kind: utils.RMDIR, DstPath: "d/s1"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test reusing a removed filename
func TestInodeReuseByTouch(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test reusing a removed filename by existing file
func TestFileReuseByMove(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
		},
		Chain{
			{
				Path: "b",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "b",
				Type: syncd.Rename,
			},
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.TOUCH, DstPath: "d/b"},
		{Kind: utils.MOVE, SrcPath: "d/b", DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

// test file copied then removed
func TestFileCopiedThenRemoved(t *testing.T) {
	content := []byte("I am Weasel!")
	hash, err := fnode.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Write,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["b"] = Chains{
		Chain{
			{
				Path: "b",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "b",
				Type: syncd.Write,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "b",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.WRITE, DstPath: "d/a", Content: content},
		{Kind: utils.COPY, DstPath: "d/b", SrcPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/b"},
	}

	runApiTest(t, &tmpFs, actions, wantedMap)
}

func runApiTest(t *testing.T, tmpFs *utils.TmpFs, actions []utils.FsAction, wantedMap DirPathToTailChainMap) {
	var err error
	if err := tmpFs.Instantiate(); err != nil {
		t.Fatal(err)
	}

	defer tmpFs.Destroy()

	// store := memstore.NewMemStore()
	dbDir := filepath.Join(tmpFs.Path, "./.db/")
	store, err := badgerstore.NewBadgerStore(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(dbDir)
	}()

	watcher := initApiTest(t, tmpFs, store)

	for i := range actions {
		if actions[i].SrcPath != "" {
			actions[i].SrcPath = filepath.Join(tmpFs.Path, actions[i].SrcPath)
		}
		if actions[i].DstPath != "" {
			actions[i].DstPath = filepath.Join(tmpFs.Path, actions[i].DstPath)
		}
	}

	takeActions(t, actions, watcher.ProcessedChanRx)
	if err := compareWanted(t, wantedMap, store); err != nil {
		t.Fatal(err)
	}
}

func initApiTest(t *testing.T, tmpFs *utils.TmpFs, store syncd.EventStore) *syncd.Watcher {
	managedDir := syncd.ManagedDirectory{Path: tmpFs.Dirs[0].Name}

	if err := setupStoreFromLocalState(tmpFs, []syncd.ManagedDirectory{managedDir}, store); err != nil {
		tmpFs.Destroy()
		t.Fatal(err)
	}

	managedMap, err := syncd.GetManagedMap(tmpFs.Path, []syncd.ManagedDirectory{managedDir})
	if err != nil {
		tmpFs.Destroy()
		t.Fatal(err)
	}

	watcher, err := syncd.InitWatcher(tmpFs.Path, []syncd.ManagedDirectory{managedDir}, managedMap, store)
	if err != nil {
		tmpFs.Destroy()
		t.Fatal(err)
	}

	go syncd.RunWatcher(watcher)

	return watcher
}

func takeActions(t *testing.T, actions []utils.FsAction, rx <-chan syncd.NodeEvent) {
	for _, action := range actions {
		n := 1
		dstExists := true
		_, err := os.Lstat(action.DstPath)
		if os.IsNotExist(err) {
			dstExists = false
		}
		err = action.Take()
		if err != nil {
			t.Fatal(err)
		}
		logger.Debug(fmt.Sprintf("Took action %+v", action))
		// after we take an action, we wait for the watcher to process the events
		// we expect a different number of events per action
		// TODO: we'll need to test scenarios where the watcher doesn't have the nodes by the time the event rolls through
		switch action.Kind {
		case utils.COPY:
			n = 1
			if !dstExists {
				n = 2
			}
		case utils.MOVE:
			n = 2
		case utils.MKDIR:
			n = 1
		case utils.REMOVE:
			n = 1
		case utils.RMDIR:
			n = 1
		case utils.TOUCH:
			n = 1
		case utils.WRITE:
			n = 1
		default:
			t.Fatal(fmt.Sprintf("unexpected utils.ActionKind: %#v", action.Kind))
		}
		nRx := 0
		firstRx := true
	forLoop:
		for {
			time.Sleep(50 * time.Millisecond) // give a safety net for processing of events
			select {
			case fileEvent := <-rx:
				nRx += 1
				logger.Trace(fmt.Sprintf("received event %+v", fileEvent))
				firstRx = false
			default:
				if !firstRx {
					break forLoop
				}
			}
		}
		if nRx != n {
			// for debug
			logger.Warn(fmt.Sprintf("received unexpected number of events %d/%d", nRx, n))
			for {
				time.Sleep(1 * time.Minute)
			}
			t.Fatal(fmt.Sprintf("received unexpected number of events %d/%d", nRx, n))
		}
	}
}

func compareWanted(t *testing.T, wantedMap DirPathToTailChainMap, store syncd.EventStore) error {
	// grab chains from store and massage into our test type
	gotMap := make(DirPathToTailChainMap)
	gotDirs := make([]string, 0)
	gotTailsMap := make(map[string][]string)

	for _, d := range store.GetDirs() {
		dirChainMap := make(TailPathToChainMap)
		gotMap[d.Path] = dirChainMap
		gotDirs = append(gotDirs, d.Path)
		gotTailsMap[d.Path] = make([]string, 0)
		chains, err := store.GetChainsInDir(d.ID)
		if chains == nil || err != nil {
			return errors.New(fmt.Sprintf("Failed to get chains in dir: %s\n%s", d, err))
		}
		for _, chain := range chains {
			events, err := store.GetEventsInChain(chain.ID)
			if events == nil || err != nil {
				return errors.New(fmt.Sprintf("Failed to get events in chain: %s\n%s", chain, err))
			}
			tailPath := events[len(events)-1].Path
			if indexOf(gotTailsMap[d.Path], tailPath) < 0 {
				gotTailsMap[d.Path] = append(gotTailsMap[d.Path], tailPath)
			}
			if _, ok := dirChainMap[tailPath]; !ok {
				dirChainMap[tailPath] = make(Chains, 0)
			}
			dirChainMap[tailPath] = append(dirChainMap[tailPath], events)
		}
	}

	// summarize wanted for top-level checks
	wantedDirs := make([]string, 0, len(wantedMap))
	wantedTailsMap := make(map[string][]string)
	for dirPath, chainMap := range wantedMap {
		wantedDirs = append(wantedDirs, dirPath)
		wantedTailsMap[dirPath] = make([]string, 0)
		for path := range chainMap {
			wantedTailsMap[dirPath] = append(wantedTailsMap[dirPath], path)
		}
	}

	// first check that the dirs wanted match the dirs got
	if !assert.ElementsMatchf(t, wantedDirs, gotDirs, "Dirs differ b/w wanted and got") {
		t.FailNow()
	}

	// then check that in each dir we got the tail files we expected
	for dirPath, wantedPaths := range wantedTailsMap {
		if !assert.ElementsMatchf(t, wantedPaths, gotTailsMap[dirPath], "Expected chain tail paths differ for %q", dirPath) {
			t.FailNow()
		}
	}

	// then test for realz
	for d, gotChainMap := range gotMap {
		wantedChainMap := wantedMap[d]
		for p, gotChains := range gotChainMap {
			wantedChains := wantedChainMap[p]
			if len(gotChains) != len(wantedChains) {
				return errors.New(fmt.Sprintf("Number of chains got differs from wanted: %d got / %d wanted", len(gotChains), len(wantedChains)))
			}
			if len(gotChains) == 1 {
				if err := eventSlicesMatch(wantedChains[0], gotChains[0]); err != nil {
					return err
				}
				continue
			}
			match := false
			for _, wantedChain := range wantedChains {
				var j int
				var gotChain Chain
				for j, gotChain = range gotChains {
					if err := eventSlicesMatch(wantedChain, gotChain); err == nil {
						match = true
						break
					}
				}
				if !match {
					return errors.New(fmt.Sprintf("No matching chains for:\n\twanted: %v\n\tgot: %v", wantedChain, gotChains))
				}
				gotChains = append(gotChains[:j], gotChains[j+1:]...)
			}
		}
	}

	return nil
}

// this function compares wanted events against got events
// it ignores hash test if hash on wanted event is nil
// it doesn't compare timestamps directly but uses to checks order of events
// doesn't check modtimes
func eventSlicesMatch(wanted []syncd.Event, got []syncd.Event) error {
	if len(wanted) != len(got) {
		return errors.New(fmt.Sprintf("Events list mismatch: len(wanted)=%d, length(got)=%d", len(wanted), len(got)))
	}

	for i, wantedEvent := range wanted {
		gotEvent := got[i]
		if !eqEvents(wantedEvent, gotEvent) {
			return errors.New(fmt.Sprintf("Events mismatch @index %d:\n\tExpected: %s\n\tGot: %s", i, wantedEvent.String(), gotEvent.String()))
		}
		if i > 0 {
			if got[i-1].Timestamp.After(gotEvent.Timestamp) {
				return errors.New(fmt.Sprintf("Events out of outer: gotEvent[%d-1].Timestamp > gotEvent[%d].Timestamp\n\t[%d-1]: %s\n\t[%d]: %s", i, i, i, got[i-1].String(), i, gotEvent.String()))
			}
		}
	}
	return nil
}

func eqEvents(wanted syncd.Event, got syncd.Event) bool {
	// always compare types
	if wanted.Type != got.Type {
		return false
	}
	// always compare paths
	if wanted.Path != got.Path {
		return false
	}
	// we don't always care about looking at node attributes, only compare if wanted has a hash
	if wanted.Hash != nil && got.Hash != nil {
		if wanted.Size != got.Size {
			return false
		}
		if *wanted.Hash != *got.Hash {
			return false
		}
	}
	return true
}

func indexOf[T comparable](slice []T, value T) int {
	for i, v := range slice {
		if v == value {
			return i
		}
	}
	return -1
}
