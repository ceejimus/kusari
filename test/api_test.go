package test

import (
	// "atmoscape.net/fileserver/syncd"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	files "atmoscape.net/fileserver/fs"
	"atmoscape.net/fileserver/logger"
	"atmoscape.net/fileserver/syncd"
	"atmoscape.net/fileserver/utils"
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

var EMPTY_HASH string

func setup() {
	logger.Init("")
	// EMPTY_HASH, err := fs.GetHash(bytes.NewReader([]byte{}))
	// if err != nil {
	// 	logger.Error(fmt.Sprintf("Failed to  make EMPTY_HASH: %s", err))
	// 	os.Exit(-1)
	// }
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
		{Kind: utils.REMOVE, SrcPath: "d/b"},
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
		{Kind: utils.REMOVE, SrcPath: "d/a"},
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
		for lkpPath, wanted := range wantedChainMap {
			dir, ok := store.GetDirByPath(dirPath)
			if !ok {
				t.Fatal("Can't get dir")
			}

			chain, ok := store.GetChainByPath(dir.ID, lkpPath)
			if !ok {
				t.Fatal("Can't get chain")
			}

			got, ok := store.GetEventsInChain(chain.ID)
			if !ok {
				t.Fatal("Can't get events")
			}

			eventSlicesMatch(t, wanted, got)
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
		// after we take an action, we wait for the watcher to process the events
		// we expect a different number of events per action
		// TODO: we'll need to test scenarios where the watcher doesn't have the nodes by the time the event rolls through
		switch action.Kind {
		case utils.COPY:
			// maybe 1 maybe 2
			t.Fatal("How many events expected from COPY?")
			n = 1
			if !dstExists {
				n = 2
			}
		case utils.MOVE:
			n = 2
		}
		for range n {
			dirEvent := <-rx
			logger.Trace(fmt.Sprintf("received event %+v", dirEvent))
		}
	}
}

// this function compares wanted events against got events
// it ignores hash test if hash on wanted event is nil
// it doesn't compare timestamps directly but uses to checks order of events
// doesn't check modtimes
func eventSlicesMatch(t *testing.T, wanted []syncd.Event, got []syncd.Event) {
	if len(wanted) != len(got) {
		t.Errorf("Events list mismatch: len(wanted)=%d, length(got)=%d", len(wanted), len(got))
	}

	for i, wantedEvent := range wanted {
		gotEvent := got[i]
		if !eqEvents(wantedEvent, gotEvent) {
			t.Errorf("Events mismatch @index %d:\n\tExpected: %s\n\tGot: %s", i, wantedEvent.String(), gotEvent.String())
		}
		if i > 0 {
			if got[i-1].Timestamp.After(gotEvent.Timestamp) {
				t.Errorf("Events out of outer: gotEvent[%d-1].Timestamp > gotEvent[%d].Timestamp\n\t[%d-1]: %s\n\t[%d]: %s", i, i, i, got[i-1].String(), i, gotEvent.String())
			}
		}
	}
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
