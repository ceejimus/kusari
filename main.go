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

	err = config.Validate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config\n%s\n", err.Error())
		os.Exit(1)
	}

	logger.Init(config.LogLevel)

	logger.Info(fmt.Sprintf("Running w/ config:%v\n", config))

	store := syncd.NewMemStore()

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

	watcher, err := syncd.InitWatcher(config.TopDir, config.ManagedDirectories, managedMap, store)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1) // TODO: gracefully handle these
	}

	go syncd.RunWatcher(watcher)

	<-make(chan bool)
}
