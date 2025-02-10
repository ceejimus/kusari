package test

import (
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
)

type Chain []syncd.Event                                 // a chain of events
type Chains []Chain                                      // a slice of chains expected (given a final path)
type TailPathToChainMap map[string]Chains                // a map of final paths to expected chains
type DirPathToTailChainMap map[string]TailPathToChainMap // a map of dir names to path -> chains lookup

// this code was mostly stolen from watcher.go
func setupStoreFromLocalState(tmpFs *utils.TmpFs, syncdDirs []syncd.SyncdDirectory, store syncd.EventStore) error {
	for _, dir := range syncdDirs {
		dirPath := filepath.Join(tmpFs.Path, dir.Path)

		newDir := syncd.Dir{
			Path: dir.Path,
		}
		err := store.AddDir(&newDir)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to add dir to event store:\n%s", err.Error()))
		}

		nodes, err := syncd.GetSyncdNodes(tmpFs.Path, dir)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to add dir to event store:\n%s", err.Error()))
		}

		// TODO: ignore nodes based on globs
		for _, node := range nodes {
			chain := syncd.Chain{
				Ino: node.Ino,
			}

			err := store.AddChain(&chain, newDir.ID)
			if err != nil {
				return errors.New(fmt.Sprintf("Failed to add event to event store:\n%s", err.Error()))
			}

			state := node.State()
			event := syncd.Event{
				Timestamp: time.Now(),
				Path:      fnode.GetRelativePath(node.Path, dirPath),
				Type:      syncd.Create,
				Size:      state.Size,
				Hash:      state.Hash,
				ModTime:   state.ModTime,
			}

			err = store.AddEvent(&event, chain.ID)
			if err != nil {
				return errors.New(fmt.Sprintf("Failed to add event to event store:\n%s", err.Error()))
			}
		}
	}

	return nil
}

func runApiTest(t *testing.T, tmpFs *utils.TmpFs, watchedDirPaths []string, actions []utils.FsAction, wantedMap DirPathToTailChainMap) {
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

	watcher := initApiTest(t, tmpFs, watchedDirPaths, store)

	for i := range actions {
		if actions[i].SrcPath != "" {
			actions[i].SrcPath = filepath.Join(tmpFs.Path, actions[i].SrcPath)
		}
		if actions[i].DstPath != "" {
			actions[i].DstPath = filepath.Join(tmpFs.Path, actions[i].DstPath)
		}
	}

	takeActions(t, actions)

	watcher.Close()

	if err := compareWanted(t, wantedMap, store); err != nil {
		t.Fatal(err)
	}
}

func initApiTest(t *testing.T, tmpFs *utils.TmpFs, watchedDirPaths []string, store syncd.EventStore) *syncd.Watcher {
	syncdDirs := make([]syncd.SyncdDirectory, len(watchedDirPaths))
	for i, watchedDirPath := range watchedDirPaths {
		syncdDirs[i] = syncd.SyncdDirectory{Path: watchedDirPath}
	}

	if err := setupStoreFromLocalState(tmpFs, syncdDirs, store); err != nil {
		tmpFs.Destroy()
		t.Fatal(err)
	}

	watcher, err := syncd.InitWatcher(tmpFs.Path, syncdDirs, store)
	if err != nil {
		tmpFs.Destroy()
		t.Fatal(err)
	}

	go watcher.Run()

	return watcher
}

func takeActions(t *testing.T, actions []utils.FsAction) {
	for _, action := range actions {
		if err := action.Take(); err != nil {
			t.Fatal(err)
		}
		logger.Debug(fmt.Sprintf("Took action %+v", action))
	}
}

func compareWanted(t *testing.T, wantedMap DirPathToTailChainMap, store syncd.EventStore) error {
	// grab chains from store and massage into our test type
	gotMap := make(DirPathToTailChainMap)
	gotDirs := make([]string, 0)
	gotTailsMap := make(map[string][]string)

	dirs, err := store.GetDirs()
	if dirs == nil || err != nil {
		return errors.New(fmt.Sprintln("Failed to get dirs"))
	}
	for _, d := range dirs {
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
