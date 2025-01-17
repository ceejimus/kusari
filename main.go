package main

import (
	"fmt"
	"os"

	"atmoscape.net/fileserver/config"
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

	logger.Init(config.LogLevel)

	logger.Info(fmt.Sprintf("Running w/ config:%v\n", config))

	managedMap, err := syncd.GetManagedMap(config.TopDir, config.ManagedDirectories)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1) // TODO: gracefully handle these
	}

	for dir, nodes := range managedMap {
		logger.Info(fmt.Sprintln(dir))
		for _, node := range nodes {
			logger.Info(fmt.Sprint(node.String()))
		}
	}

	watcher := syncd.InitWatcher(config.TopDir, config.ManagedDirectories, managedMap)
	go syncd.RunWatcher(watcher)

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

// func pollFileState(tick <-chan time.Time, done <-chan bool, states chan<- *NodeState, config *NodeConfig) {
// 	run := func() {
// 		managedMap, err := getManagedMap(config.TopDir, config.ManagedDirectories)
// 		if err != nil {
// 			logger.Error(err.Error())
// 			os.Exit(1) // TODO: gracefully handle these
// 		}
//
// 		for managedDir, managedFiles := range managedMap {
// 			logger.Debug(fmt.Sprintf("ManagedDir: %v\n", managedDir))
// 			for _, fileState := range managedFiles {
// 				states <- &fileState
// 			}
// 		}
// 	}
//
// 	ready := make(chan bool, 1)
// 	ready <- true // so we start
// 	for {
// 		select {
// 		case <-done:
// 			return
// 		case _ = <-tick:
// 			select {
// 			case _ = <-ready:
// 				go func() {
// 					run()
// 					ready <- true
// 				}()
// 			default:
// 				logger.Info(fmt.Sprintln("FileState poller congestion..."))
// 			}
// 		}
// 	}
// }
//
// // TODO: batching
// // TODO: gracefully handle errors
// func upsertFileStates(done <-chan bool, states <-chan *NodeState, db *DB) {
// 	for {
// 		select {
// 		case <-done:
// 			return
// 		case fileState := <-states:
// 			logger.Trace(fmt.Sprintf("Upserting FileState: %+v\n", fileState))
// 			err := db.UpsertFileState(fileState)
// 			if err != nil {
// 				logger.Error(err.Error())
// 				os.Exit(1)
// 			}
// 		}
// 	}
// }
