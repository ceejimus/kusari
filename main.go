package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type NodeConfig struct {
	LogLevel           string             `yaml:"logLevel"`
	ManagedDirectories []ManagedDirectory `yaml:"dirs"`
}

const CONFIG_YAML_PATH = "./.data/cnf.yaml"

var logger Logger

func main() {
	config, err := loadConfig(CONFIG_YAML_PATH)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse YAML config @ '%s'\n%s\n", CONFIG_YAML_PATH, err)
		os.Exit(1)
	}

	logger, err = makeLogger(config.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse create logger @ '%s'\n%s\n", config.LogLevel, err)
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("Running w/ config:%v\n", config))

	homedir, err := os.UserHomeDir()
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to retrieve user home directory: \n%v\n", err))
	}

	logger.Debug(fmt.Sprintf("Initializing files relative to user home dir: %v\n", homedir))

	managedMap, err := makeManagedMap(homedir, config.ManagedDirectories)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1) // TODO: gracefully handle these
	}

	for managedDir, managedFiles := range managedMap {
		logger.Debug(fmt.Sprintf("ManagedDir: %v\n", managedDir))
		for _, managedFile := range managedFiles {
			logger.Debug(fmt.Sprintf(" - %v\n", managedFile))
		}
	}
}

func makeLogger(levelStr string) (Logger, error) {
	level, err := Logger{}.ParseLogLevel(levelStr)
	if err != nil {
		return Logger{}, err
	}
	logger := Logger{}.New(level)
	return logger, nil
}

func loadConfig(filename string) (*NodeConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config NodeConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
