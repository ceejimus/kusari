package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"io/fs"

	"github.com/gobwas/glob"
)

type ManagedDirectory struct {
	Path    string   `yaml:"path"`
	Include []string `yaml:"incl"`
	Exclude []string `yaml:"excl"`
}

type FileState struct {
	Path    string
	Updated time.Time
	ModTime time.Time
	Size    int64
	Hash    string
}

type ManagedMap map[string]map[string][]FileState

func makeManagedMap(homedir string, managedDirs []ManagedDirectory) (ManagedMap, error) {
	managedMap := make(ManagedMap)

	for _, managedDir := range managedDirs {
		managedFiles, err := getManagedFiles(homedir, managedDir)
		if err != nil {
			return nil, err
		}
		managedMap[managedDir.Path] = managedFiles
	}

	return managedMap, nil
}

func getManagedFiles(homedir string, managedDir ManagedDirectory) (map[string][]FileState, error) {
	managedFiles := make(map[string][]FileState)

	inclGlobs := mapToGlobs(managedDir.Include)
	exclGlobs := mapToGlobs(managedDir.Exclude)

	err := filepath.WalkDir(filepath.Join(homedir, managedDir.Path), func(path string, d fs.DirEntry, err error) error {

		if err != nil {
			return errors.New(fmt.Sprintf("Failure when walking dir: %v\npath: %v\n%v\n", managedDir.Path, path, err))
		}

		// we're only going to look at regular files for now
		// TODO: via config, have the sync store sources for links that are below user home and manage those too
		// TODO: implement our own directory recursion?
		if fs.ModeType&d.Type() != 0 {
			logger.Debug(fmt.Sprintf("SKIPPING - %v : %v : %v", path, d, err))
		}

		if checkGlobs(exclGlobs, d.Name(), false) {
			logger.Debug(fmt.Sprintf("Excluded - %v : %v : %v", path, d, err))
			return nil
		}

		if !checkGlobs(inclGlobs, d.Name(), true) {
			logger.Debug(fmt.Sprintf("Not included - %v : %v : %v", path, d, err))
			return nil
		}

		fileinfo, err := d.Info()
		if err != nil {
			return err
		}

		filestate, err := getFileState(path, fileinfo)
		if err != nil {
			return err
		}

		managedFiles[path] = []FileState{*filestate}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return managedFiles, nil
}

// fileinfo, err := os.Lstat(path)
// if err != nil {
// 	return nil, err
// }

// func getFileStates(managedMap

func mapToGlobs(globStrs []string) []glob.Glob {
	globs := make([]glob.Glob, len(globStrs))
	for i, globStr := range globStrs {
		globs[i] = glob.MustCompile(globStr)
	}
	return globs
}

func checkGlobs(globs []glob.Glob, input string, onEmpty bool) bool {
	if len(globs) == 0 {
		return onEmpty
	}

	for _, g := range globs {
		if g.Match(input) {
			return true
		}
	}

	return false
}

func getFileState(path string, fileinfo fs.FileInfo) (*FileState, error) {
	filehash, err := fileHash(path)
	if err != nil {
		return nil, err
	}

	return &FileState{
		Path:    path,
		Updated: time.Now(),
		ModTime: fileinfo.ModTime(),
		Size:    fileinfo.Size(),
		Hash:    filehash,
	}, nil
}

func fileHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
