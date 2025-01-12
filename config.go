package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	DSN                string             `yaml:"dsn"`
	LogLevel           string             `yaml:"logLevel"`
	TopDir             string             `yaml:"topdir"`
	ManagedDirectories []ManagedDirectory `yaml:"dirs"`
}

const CONFIG_YAML_PATH = "./.data/cnf.yaml"

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
