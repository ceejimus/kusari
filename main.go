package main

import (
	"fmt"
	"os"
	"time"
)

var logger Logger

func main() {
	config, err := loadConfig(CONFIG_YAML_PATH)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse YAML config @ '%s'\n%s\n", CONFIG_YAML_PATH, err)
		os.Exit(1)
	}

	logger = makeLogger(config.LogLevel)

	logger.Info(fmt.Sprintf("Running w/ config:%v\n", config))

	managedMap, err := getManagedDirectoryFileStates(config.TopDir, config.ManagedDirectories)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1) // TODO: gracefully handle these
	}

	watcher := initWatcher(config)
	go runWatcher(watcher)

	// db, err := initDb(config.DSN)
	// if err != nil {
	// 	logger.Error(err.Error())
	// 	os.Exit(1) // TODO: gracefully handle these
	// }

	// results, err := db.GetFileStateByPath("syncfile.txt")
	//
	// for _, r := range results {
	// 	fmt.Printf("%+v\n", r)
	// }
	//
	// os.Exit(0)

	// fileStatePollTicker := time.NewTicker(1 * time.Second)
	// pollFileStateDone := make(chan bool, 1)
	// fileStateChannel := make(chan *FileState, 32)
	//
	// defer fileStatePollTicker.Stop()
	//
	// go pollFileState(fileStatePollTicker.C, pollFileStateDone, fileStateChannel, config)
	//
	// upsertFileStateDone := make(chan bool, 1)
	// go upsertFileStates(upsertFileStateDone, fileStateChannel, db)

	// go runApi(config, db)

	<-make(chan bool)
}

func pollFileState(tick <-chan time.Time, done <-chan bool, states chan<- *FileState, config *NodeConfig) {
	run := func() {
		managedMap, err := getManagedDirectoryFileStates(config.TopDir, config.ManagedDirectories)
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1) // TODO: gracefully handle these
		}

		for managedDir, managedFiles := range managedMap {
			logger.Debug(fmt.Sprintf("ManagedDir: %v\n", managedDir))
			for _, fileState := range managedFiles {
				states <- &fileState
			}
		}
	}

	ready := make(chan bool, 1)
	ready <- true // so we start
	for {
		select {
		case <-done:
			return
		case _ = <-tick:
			select {
			case _ = <-ready:
				go func() {
					run()
					ready <- true
				}()
			default:
				logger.Info(fmt.Sprintln("FileState poller congestion..."))
			}
		}
	}
}

// TODO: batching
// TODO: gracefully handle errors
func upsertFileStates(done <-chan bool, states <-chan *FileState, db *DB) {
	for {
		select {
		case <-done:
			return
		case fileState := <-states:
			logger.Trace(fmt.Sprintf("Upserting FileState: %+v\n", fileState))
			err := db.UpsertFileState(fileState)
			if err != nil {
				logger.Error(err.Error())
				os.Exit(1)
			}
		}
	}
}
