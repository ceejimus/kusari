package test

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	files "atmoscape.net/fileserver/fs"
	"atmoscape.net/fileserver/syncd"
	"atmoscape.net/fileserver/utils"
)

type MemStoreInitState struct{}

type MemStoreDirState struct{}

type MemStoreChainState struct{}

// func InitMemStore() (*syncd.MemStore, error) {
//
// }

// this code was mostly stolen from watcher.go
// this will evolve into a more flexible solution for testing purposes
func setupStoreFromLocalState(tmpFs *utils.TmpFs, managedDirs []syncd.ManagedDirectory, store syncd.EventStore) error {
	for _, dir := range managedDirs {
		dirPath := filepath.Join(tmpFs.Path, dir.Path)

		newDir := &syncd.Dir{
			Path: dir.Path,
		}
		newDir, err := store.AddDir(*newDir)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to add dir to event store:\n%s", err.Error()))
		}

		nodes, err := syncd.GetManagedNodes(tmpFs.Path, dir)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to add dir to event store:\n%s", err.Error()))
		}

		// TODO: ignore nodes based on globs
		for _, node := range nodes {
			chain := &syncd.Chain{
				Ino: node.Ino(),
			}

			newChain, err := store.AddChain(*chain, newDir.ID)
			if err != nil {
				return errors.New(fmt.Sprintf("Failed to add event to event store:\n%s", err.Error()))
			}

			state := node.State()
			event := &syncd.Event{
				Timestamp: time.Now(),
				Path:      files.GetRelativePath(node.Path, dirPath),
				Type:      "create",
				Size:      state.Size,
				Hash:      state.Hash,
				ModTime:   state.ModTime,
			}

			_, err = store.AddEvent(*event, newChain.ID)
			if err != nil {
				return errors.New(fmt.Sprintf("Failed to add event to event store:\n%s", err.Error()))
			}
		}
	}

	return nil
}
