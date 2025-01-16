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
	"syscall"
	"time"

	"io/fs"

	"github.com/gobwas/glob"
)

type NodeType int

const (
	FILE NodeType = iota + 1
	DIR
)

// static metadata about a node
type NodeMeta struct {
	Type NodeType // file, dir, link ,etc
	Ino  uint64   // inode
}

// TODO: think about what goes in NodeMeta
// dynamic node state state
type NodeState struct {
	Path    string
	Hash    *string
	Size    int64
	ModTime time.Time
}

// TODO make sure this stays sorted
type Node struct {
	Meta  NodeMeta  // node metadata
	State NodeState // node state
}

type ManagedDirectory struct {
	Path    string   `yaml:"path"`
	Include []string `yaml:"incl"`
	Exclude []string `yaml:"excl"`
}

type ManagedMap map[string][]NodeState

func getManagedMap(topdir string, managedDirs []ManagedDirectory) (ManagedMap, error) {
	managedMap := make(map[string][]NodeState)

	for _, managedDir := range managedDirs {
		managedFiles, err := getManagedNodes(topdir, managedDir)
		if err != nil {
			return nil, err
		}

		managedMap[managedDir.Path] = managedFiles
	}

	return managedMap, nil
}

func getManagedNodes(topdir string, managedDir ManagedDirectory) ([]NodeState, error) {
	managedFiles := make([]NodeState, 0)

	inclGlobs := mapToGlobs(managedDir.Include)
	exclGlobs := mapToGlobs(managedDir.Exclude)

	fullDirPath := filepath.Join(topdir, managedDir.Path)

	logger.Trace(fmt.Sprintf("topdir: %q - managedDir.Path: %q - %q\n", topdir, managedDir.Path, fullDirPath))
	err := filepath.WalkDir(fullDirPath, func(path string, d fs.DirEntry, err error) error {

		if err != nil {
			return errors.New(fmt.Sprintf("Failure when walking dir: %v\npath: %v\n%v\n", managedDir.Path, path, err))
		}

		relPath := getRelativePath(path, fullDirPath)

		// for now we only check globs on files
		isDir := d.Type().IsDir()
		if !isDir && checkGlobs(exclGlobs, relPath, false) {
			logger.Trace(fmt.Sprintf("Excluded - %v : %v", relPath, d))
			return nil
		}

		if !isDir && !checkGlobs(inclGlobs, relPath, true) {
			logger.Trace(fmt.Sprintf("Not included - %v : %v", relPath, d))
			return nil
		}

		// we're only going to look at regular files and regular dirs
		// TODO: implement our own directory recursion?
		if !d.Type().IsRegular() && !d.Type().IsDir() {
			logger.Trace(fmt.Sprintf("SKIPPING - %v : %v", relPath, d))
			return nil
		}

		logger.Trace(fmt.Sprintf("Adding - %v : %v", relPath, d))

		fileinfo, err := d.Info()
		if err != nil {
			return err
		}

		logger.Trace(fmt.Sprintf("%q", relPath))
		filestate, err := getNodeState(relPath, path, fileinfo)
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

func getRelativePath(fullPath string, relDir string) string {
	if !strings.HasSuffix(relDir, "/") {
		relDir = fmt.Sprintf("%v/", relDir)
	}
	return strings.Replace(fullPath, relDir, "", 1)
}

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

func getNodeState(relPath string, path string, info fs.FileInfo) (*NodeState, error) {
	var hash *string

	size := int64(0)

	meta, err := getNodeMeta(info)

	if meta.Type == FILE {
		filehash, err := fileHash(path)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to hash file %q\n%v\n", path, err.Error()))
		}
		hash = &filehash

		size = info.Size()
	}

	if err != nil {
		return nil, err
	}

	return &NodeState{
		Path:    relPath,
		Size:    size,
		Hash:    hash,
		ModTime: info.ModTime(),
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

func getNodeMeta(info fs.FileInfo) (*NodeMeta, error) {
	// logger.Trace(fmt.Sprintf("FileMode for %q | %v", path, info.Mode()))
	nodeType := getNodeType(info.Mode())

	if nodeType < 1 {
		// unsupported node
		return nil, nil
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.ErrUnsupported
	}

	return &NodeMeta{
		Type: nodeType,
		Ino:  stat.Ino,
	}, nil
}

func getNodeType(fileMode fs.FileMode) NodeType {
	if fileMode.IsRegular() {
		return FILE
	}

	if fileMode.IsDir() {
		return DIR
	}

	return -1
}
