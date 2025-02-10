package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"atmoscape.net/fileserver/syncd"
	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	DSN              string                 `yaml:"dsn"`
	LogLevel         string                 `yaml:"logLevel"`
	TopDir           string                 `yaml:"topDir"`
	SyncdDirectories []syncd.SyncdDirectory `yaml:"dirs"`
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

	config.TopDir = filepath.Clean(config.TopDir)
	for i := range config.SyncdDirectories {
		config.SyncdDirectories[i].Path = filepath.Clean(config.SyncdDirectories[i].Path)
	}

	return &config, nil
}

func (cnf *NodeConfig) Validate() error {
	err := checkDir(cnf.TopDir)
	if err != nil {
		return errors.New(fmt.Sprintf("Invalid TopDir - %s", err.Error()))
	}

	for _, dir := range cnf.SyncdDirectories {
		fullPath := filepath.Join(cnf.TopDir, dir.Path)
		err := checkDir(fullPath)
		if err != nil {
			return errors.New(fmt.Sprintf("Invalid SyncdDirectory path - %s", err.Error()))
		}
	}

	return nil
}

func checkDir(path string) error {
	if path == "" {
		return errors.New("Empty path")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to stat path: %q", path))
	}

	if !info.IsDir() {
		return errors.New(fmt.Sprintf("path is not a directory: %q", path))
	}

	return nil
}
