package test

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"atmoscape.net/fileserver/fnode"
	"atmoscape.net/fileserver/syncd"
	"atmoscape.net/fileserver/utils"
)

// this code was mostly stolen from watcher.go
func setupStoreFromLocalState(tmpFs *utils.TmpFs, managedDirs []syncd.ManagedDirectory, store syncd.EventStore) error {
	for _, dir := range managedDirs {
		dirPath := filepath.Join(tmpFs.Path, dir.Path)

		newDir := syncd.Dir{
			Path: dir.Path,
		}
		err := store.AddDir(&newDir)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to add dir to event store:\n%s", err.Error()))
		}

		nodes, err := syncd.GetManagedNodes(tmpFs.Path, dir)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to add dir to event store:\n%s", err.Error()))
		}

		// TODO: ignore nodes based on globs
		for _, node := range nodes {
			chain := syncd.Chain{
				Ino: node.Ino(),
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
