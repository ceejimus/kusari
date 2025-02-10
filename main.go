package main

import (
	"fmt"
	"os"

	"atmoscape.net/fileserver/badgerstore"
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
}
