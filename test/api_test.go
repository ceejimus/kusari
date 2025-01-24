package test

import (
	// "atmoscape.net/fileserver/syncd"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	files "atmoscape.net/fileserver/fs"
	"atmoscape.net/fileserver/logger"
	"atmoscape.net/fileserver/syncd"
	"atmoscape.net/fileserver/utils"
	"golang.org/x/sys/unix"
)

type ApiTestWrapper struct {
	watcher *syncd.Watcher
	store   syncd.EventStore
}

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
		logger.Error(fmt.Sprintf("Faild to stat wd %q: %s", wd, err))
		os.Exit(-1)
	}
	DIR_BLOCK_SIZE = uint64(statfs.Bsize)
}

// test a create, write, rename, remove flow for single file
func TestSingleFileCWRR(t *testing.T) {
	content := []byte("I am Weasel!")
	hash, err := files.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(map[string]map[string][]syncd.Event)
	wantedMap["d"] = make(map[string][]syncd.Event)
	wantedMap["d"]["b"] = []syncd.Event{
		{
			Path: "a",
			Type: "create",
			Size: 0,
		},
		{
			Path: "a",
			Type: "write",
			Size: uint64(len(content)),
			Hash: &hash,
		},
		{
			Path: "a",
			Type: "rename",
		},
		{
			Path: "b",
			Type: "create",
			Size: uint64(len(content)),
			Hash: &hash,
		},
		{
			Path: "b",
			Type: "remove",
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

// test a remove for a single file
func TestExistingFileRemove(t *testing.T) {
	content := []byte("i am a")
	hash, err := files.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(map[string]map[string][]syncd.Event)
	wantedMap["d"] = make(map[string][]syncd.Event)
	wantedMap["d"]["a"] = []syncd.Event{
		{
			Path: "a",
			Type: "create",
			Size: uint64(len(content)),
			Hash: &hash,
		},
		{
			Path: "a",
			Type: "remove",
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

// test creating a file w/ same old path as renamed file
func TestTouchMovedFilename(t *testing.T) {
	wantedMap := make(map[string]map[string][]syncd.Event)
	wantedMap["d"] = make(map[string][]syncd.Event)
	wantedMap["d"]["b"] = []syncd.Event{
		{
			Path: "a",
			Type: "create",
			Size: 0,
		},
		{
			Path: "a",
			Type: "rename",
		},
		{
			Path: "b",
			Type: "create",
			Size: 0,
		},
		{
			Path: "b",
			Type: "remove",
		},
	}
	wantedMap["d"]["a"] = []syncd.Event{
		{
			Path: "a",
			Type: "create",
			Size: 0,
		},
		{
			Path: "a",
			Type: "remove",
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
	wantedMap := make(map[string]map[string][]syncd.Event)
	wantedMap["d"] = make(map[string][]syncd.Event)
	wantedMap["d"]["s1"] = []syncd.Event{
		{
			Path: "s1",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s1",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2"] = []syncd.Event{
		{
			Path: "s2",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s2",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2/a"] = []syncd.Event{
		{
			Path: "s1/a",
			Type: "create",
			Size: 0,
		},
		{
			Path: "s1/a",
			Type: "rename",
		},
		{
			Path: "s2/a",
			Type: "create",
			Size: 0,
		},
		{
			Path: "s2/a",
			Type: "remove",
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
	wantedMap := make(map[string]map[string][]syncd.Event)
	wantedMap["d"] = make(map[string][]syncd.Event)
	wantedMap["d"]["s1"] = []syncd.Event{
		{
			Path: "s1",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s1",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2"] = []syncd.Event{
		{
			Path: "s2",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s2",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2/s3"] = []syncd.Event{
		{
			Path: "s1/s3",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s1/s3",
			Type: "rename",
		},
		{
			Path: "s2/s3",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s2/s3",
			Type: "remove",
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
	wantedMap := make(map[string]map[string][]syncd.Event)
	wantedMap["d"] = make(map[string][]syncd.Event)
	wantedMap["d"]["s1"] = []syncd.Event{
		{
			Path: "s1",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s1",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2"] = []syncd.Event{
		{
			Path: "s2",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s2",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2/s3"] = []syncd.Event{
		{
			Path: "s1/s3",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s1/s3",
			Type: "rename",
		},
		{
			Path: "s2/s3",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s2/s3",
			Type: "remove",
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
func TestMoveNonEmptySubdirThenMoveFileOut(t *testing.T) {
	wantedMap := make(map[string]map[string][]syncd.Event)
	wantedMap["d"] = make(map[string][]syncd.Event)
	wantedMap["d"]["s1"] = []syncd.Event{
		{
			Path: "s1",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s1",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2"] = []syncd.Event{
		{
			Path: "s2",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s2",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2/s4"] = []syncd.Event{
		{
			Path: "s2/s4",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s2/s4",
			Type: "remove",
		},
	}
	wantedMap["d"]["s2/s4/s3"] = []syncd.Event{
		{
			Path: "s1/s3",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s1/s3",
			Type: "rename",
		},
		{
			Path: "s2/s4/s3",
			Type: "create",
			Size: DIR_BLOCK_SIZE,
		},
		{
			Path: "s2/s4/s3",
			Type: "remove",
		},
	}
	wantedMap["d"]["s1/a"] = []syncd.Event{
		{
			Path: "s1/s3/a",
			Type: "create",
			Size: 0,
		},
		{
			Path: "s2/s4/s3/a",
			Type: "rename",
		},
		{
			Path: "s1/a",
			Type: "create",
			Size: 0,
		},
		{
			Path: "s1/a",
			Type: "remove",
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

func runApiTest(t *testing.T, tmpFs *utils.TmpFs, actions []utils.FsAction, wantedMap map[string]map[string][]syncd.Event) {
	wrapper := initApiTest(t, tmpFs)
	watcher := wrapper.watcher
	store := wrapper.store
	defer tmpFs.Destroy()

	for i := range actions {
		if actions[i].SrcPath != "" {
			actions[i].SrcPath = filepath.Join(tmpFs.Path, actions[i].SrcPath)
		}
		if actions[i].DstPath != "" {
			actions[i].DstPath = filepath.Join(tmpFs.Path, actions[i].DstPath)
		}
	}

	takeActions(t, actions, watcher.ProcessedChanRx)

	for dirPath, wantedChainMap := range wantedMap {
		dir, ok := store.GetDirByPath(dirPath)
		if !ok {
			t.Fatalf("Can't get dir: %q", dirPath)
		}

		gotChains, ok := store.GetChainsInDir(dir.ID)
		if !ok {
			t.Fatalf("Can't get chains for dir: %q", dirPath)
		}

		for lkpPath, wanted := range wantedChainMap {
			chain, ok := store.GetChainByPath(dir.ID, lkpPath)
			if !ok {
				t.Fatal("Can't get chain")
			}

			got, ok := store.GetEventsInChain(chain.ID)
			if !ok {
				t.Fatal("Can't get events")
			}

			if err := eventSlicesMatch(wanted, got); err != nil {
				logger.Error(fmt.Sprintf("events mismatch testing %s/%s", dirPath, lkpPath))
				t.Fatal(err)
			}
		}

		if len(gotChains) != len(wantedChainMap) {
			t.Fatalf("Got chains we didn't want:\n\tgot: %+v\n\twanted: %+v", gotChains, wantedChainMap)
		}
	}
}

func initApiTest(t *testing.T, tmpFs *utils.TmpFs) ApiTestWrapper {
	var err error
	if err := tmpFs.Instantiate(); err != nil {
		t.Fatal(err)
	}

	store := syncd.NewMemStore()
	managedDir := syncd.ManagedDirectory{Path: "d"}

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

	return ApiTestWrapper{store: store, watcher: watcher}
}

func takeActions(t *testing.T, actions []utils.FsAction, rx <-chan syncd.FileEvent) {
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
			// logger.Warn(fmt.Sprintf("received unexpected number of events %d/%d", nRx, n))
			// for {
			// 	time.Sleep(1 * time.Minute)
			// }
			t.Fatal(fmt.Sprintf("received unexpected number of events %d/%d", nRx, n))
		}
	}
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
	if wanted.Type != got.Type {
		return false
	}
	if wanted.Path != got.Path {
		return false
	}
	if wanted.Size != got.Size {
		return false
	}
	if wanted.Hash != nil && got.Hash != nil && *wanted.Hash != *got.Hash {
		return false
	}
	return true
}
