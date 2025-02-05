package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"atmoscape.net/fileserver/badgerstore"
	"atmoscape.net/fileserver/config"
	"atmoscape.net/fileserver/fnode"
	"atmoscape.net/fileserver/logger"
	"atmoscape.net/fileserver/syncd"
)

const CONFIG_YAML_PATH = "./.data/cnf.yaml"

func main() {
	config, err := config.LoadConfig(CONFIG_YAML_PATH)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse YAML config @ '%s'\n%s\n", CONFIG_YAML_PATH, err)
		os.Exit(1)
	}

	err = config.Validate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config\n%s\n", err.Error())
		os.Exit(1)
	}

	logger.Init(config.LogLevel)

	logger.Info(fmt.Sprintf("Running w/ config:%v\n", config))
	dbDir := "./.data/db/"
	dir := syncd.Dir{Path: "d"}
	badgerStore, err := badgerstore.NewBadgerStore(dbDir)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	if err = badgerStore.AddDir(&dir); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	logger.Debug(fmt.Sprintf("New Dir: %+v", dir))
	gotDir, err := badgerStore.GetDirByID(dir.ID)
	if gotDir == nil || err != nil {
		logger.Error("couldn't get dir")
		os.Exit(1)
	}
	logger.Debug(fmt.Sprintf("Got Dir: %+v", gotDir))
	//
	// store := syncd.NewMemStore()
	//
	// managedMap, err := syncd.GetManagedMap(config.TopDir, config.ManagedDirectories)
	// if err != nil {
	// 	logger.Fatal(err.Error())
	// 	os.Exit(1) // TODO: gracefully handle these
	// }
	//
	// for _, dir := range config.ManagedDirectories {
	// 	// update event store w/ current directory state
	// 	nodes := managedMap[dir.Path]
	// 	_, err := updateStoreForLocalState(config.TopDir, dir, nodes, store)
	// 	if err != nil {
	// 		logger.Fatal(err.Error())
	// 		os.Exit(1)
	// 	}
	// }
	//
	// watcher, err := syncd.InitWatcher(config.TopDir, config.ManagedDirectories, managedMap, store)
	// if err != nil {
	// 	logger.Fatal(err.Error())
	// 	os.Exit(1) // TODO: gracefully handle these
	// }
	//
	// go syncd.RunWatcher(watcher)
	//
	// <-make(chan bool)
}

func updateStoreForLocalState(topDir string, managedDir syncd.ManagedDirectory, nodes []fnode.Node, store syncd.EventStore) (*syncd.Dir, error) {
	// TODO: ignore events based on globs
	dirName := filepath.Join(topDir, managedDir.Path)

	dir, err := store.GetDirByPath(managedDir.Path)
	if err != nil {
		return nil, err
	}

	if dir == nil {
		dir = &syncd.Dir{
			Path: managedDir.Path,
		}
		err := store.AddDir(dir)
		if err != nil {
			logger.Fatal(fmt.Sprintf("Failed to add dir to event store:\n%s", err.Error()))
			os.Exit(1)
		}
	}

	// populate event logs w/ create events
	for _, node := range nodes {
		state := node.State()

		chain, err := store.GetChainByPath(dir.ID, fnode.GetRelativePath(node.Path, dirName))
		if err != nil {
			return nil, err
		}

		if chain == nil {
			chain = &syncd.Chain{
				Ino: node.Ino(),
			}

			err := store.AddChain(chain, dir.ID)
			if err != nil {
				return nil, err
			}

			event := syncd.Event{
				Timestamp: time.Now(),
				Path:      fnode.GetRelativePath(node.Path, dirName),
				Type:      syncd.Create,
				Size:      state.Size,
				Hash:      state.Hash,
				ModTime:   state.ModTime,
			}

			if err = store.AddEvent(&event, chain.ID); err != nil {
				return nil, err
			}
		}
	}

	return dir, nil
}
