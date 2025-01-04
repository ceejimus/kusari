package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	LogLevel           string `yaml:"logLevel"`
	ManagedDirectories []struct {
		Path    string   `yaml:"path"`
		Include []string `yaml:"incl"`
		Exclude []string `yaml:"excl"`
	} `yaml:"dirs"`
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

	fmt.Println(config)

	logger.Debug("Debug Test")
	logger.Info("Info Test")
	logger.Warn("Warn Test")
	logger.Error("Error Test")
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
