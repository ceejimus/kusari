package main

import (
	"fmt"
	"os"

	"github.com/ceejimus/kusari/config"
	"github.com/ceejimus/kusari/logger"
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
}
