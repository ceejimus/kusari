package config

import (
	"os"

	"atmoscape.net/fileserver/syncd"
	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	DSN                string                   `yaml:"dsn"`
	LogLevel           string                   `yaml:"logLevel"`
	TopDir             string                   `yaml:"topDir"`
	ManagedDirectories []syncd.ManagedDirectory `yaml:"dirs"`
}

func LoadConfig(filename string) (*NodeConfig, error) {
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
