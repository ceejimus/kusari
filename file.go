package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	Path      string
	Hash      string
	Size      int64
	ModTime   time.Time
	Timestamp time.Time
}

type ManagedMap map[string]map[string]FileState

func getManagedDirectoryFileStates(topdir string, managedDirs []ManagedDirectory) (map[string][]FileState, error) {
	managedMap := make(map[string][]FileState)

	for _, managedDir := range managedDirs {
		managedFiles, err := getManagedFiles(topdir, managedDir)
		if err != nil {
			return nil, err
		}

		managedMap[managedDir.Path] = managedFiles
	}

	return managedMap, nil
}

func getManagedFiles(topdir string, managedDir ManagedDirectory) ([]FileState, error) {
	managedFiles := make([]FileState, 0)

	inclGlobs := mapToGlobs(managedDir.Include)
	exclGlobs := mapToGlobs(managedDir.Exclude)

	fullPath := filepath.Join(topdir, managedDir.Path)

	logger.Trace(fmt.Sprintf("homedir: %v - managedDir.Path: %v - %v\n", topdir, managedDir.Path, fullPath))
	err := filepath.WalkDir(fullPath, func(path string, d fs.DirEntry, err error) error {

		if err != nil {
			return errors.New(fmt.Sprintf("Failure when walking dir: %v\npath: %v\n%v\n", managedDir.Path, path, err))
		}

		localPath := relPath(path, fullPath)

		// we're only going to look at regular files for now
		// TODO: via config, have the sync store sources for links that are below user home and manage those too
		// TODO: implement our own directory recursion?
		if fs.ModeType&d.Type() != 0 {
			logger.Trace(fmt.Sprintf("SKIPPING - %v : %v", localPath, d))
			return nil
		}

		if checkGlobs(exclGlobs, localPath, false) {
			logger.Trace(fmt.Sprintf("Excluded - %v : %v", localPath, d))
			return nil
		}

		if !checkGlobs(inclGlobs, localPath, true) {
			logger.Trace(fmt.Sprintf("Not included - %v : %v", localPath, d))
			return nil
		}

		logger.Trace(fmt.Sprintf("Adding - %v : %v", localPath, d))

		fileinfo, err := d.Info()
		if err != nil {
			return err
		}

		filestate, err := getFileState(path, fileinfo)
		filestate.Path = relPath(filestate.Path, fullPath)
		if err != nil {
			return err
		}

		managedFiles = append(managedFiles, *filestate)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return managedFiles, nil
}

func relPath(fullPath string, relDir string) string {
	if !strings.HasSuffix(relDir, "/") {
		relDir = fmt.Sprintf("%v/", relDir)
	}
	return strings.Replace(fullPath, relDir, "", 1)
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

	// logger.Trace(fmt.Sprintf("filehash: %v, len: %d\n", filehash, len(filehash)))

	return &FileState{
		Path:      path,
		Timestamp: time.Now(),
		ModTime:   fileinfo.ModTime(),
		Size:      fileinfo.Size(),
		Hash:      filehash,
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
