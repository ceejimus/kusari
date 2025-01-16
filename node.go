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

type ManagedDirectory struct {
	Path    string   `yaml:"path"`
	Include []string `yaml:"incl"`
	Exclude []string `yaml:"excl"`
}

type ManagedMap map[string][]Node

type NodeState struct {
	Path    string
	ModTime time.Time
	Hash    string
	Size    uint64
}

type Node struct {
	info fs.FileInfo
	Path string
}

func (n *Node) Type() NodeType {
	// extract type from info and set on type
	mode := n.info.Mode()
	if mode.IsRegular() {
		return FILE
	} else if mode.IsDir() {
		return DIR
	}
	return -1
}

func (n *Node) Ino() uint64 {
	// extract inode from stat
	stat, ok := n.info.Sys().(*syscall.Stat_t)
	if ok {
		return stat.Ino
	}
	return 0
}

func (n *Node) Size() uint64 {
	return uint64(n.info.Size())
}

func (n *Node) Modtime() time.Time {
	return n.info.ModTime()
}

func (n *Node) Hash() (string, error) {
	if n.Type() != FILE {
		return "", errors.ErrUnsupported
	}

	hash, err := fileHash(n.Path)
	if err != nil {
		return "", err
	}

	return hash, nil
}

func (n *Node) State() NodeState {
	hash, _ := n.Hash()
	return NodeState{
		Path:    n.Path,
		ModTime: n.Modtime(),
		Hash:    hash,
		Size:    n.Size(),
	}
}

func (n *Node) String() string {
	var b strings.Builder
	nodeTypeStr := "\\"
	if n.Type() == FILE {
		nodeTypeStr = "-"
	} else if n.Type() == DIR {
		nodeTypeStr = "d"
	}
	b.WriteString(fmt.Sprintf("(%.8d) ", n.Ino()))
	b.WriteString(fmt.Sprintf("%v ", nodeTypeStr))
	b.WriteString(fmt.Sprintf("%.8d", n.Size()))
	//	01/02 03:04:05PM '06 -0700
	//	Mon Jan 2 15:04:05 MST 2006
	b.WriteString(fmt.Sprintf(" %s", n.Modtime().Format("2006-01-02 15:04:05 MST")))
	b.WriteString(fmt.Sprintf(" %s", n.info.Name()))
	hash, err := n.Hash()
	if err == nil {
		b.WriteString(fmt.Sprintf(" |%s|", hash))
	}
	return b.String()
}

func getManagedMap(topdir string, managedDirs []ManagedDirectory) (ManagedMap, error) {
	managedMap := make(map[string][]Node)

	for _, managedDir := range managedDirs {
		managedFiles, err := getManagedNodes(topdir, managedDir)
		if err != nil {
			return nil, err
		}

		managedMap[managedDir.Path] = managedFiles
	}

	return managedMap, nil
}

func getManagedNodes(topdir string, managedDir ManagedDirectory) ([]Node, error) {
	managedFiles := make([]Node, 0)

	inclGlobs := mapToGlobs(managedDir.Include)
	exclGlobs := mapToGlobs(managedDir.Exclude)

	fullDirPath := filepath.Join(topdir, managedDir.Path)

	logger.Trace(fmt.Sprintf("topdir: %q - managedDir.Path: %q - %q\n", topdir, managedDir.Path, fullDirPath))
	err := filepath.WalkDir(fullDirPath, func(path string, d fs.DirEntry, err error) error {

		if err != nil {
			return errors.New(fmt.Sprintf("Failure when walking dir: %v\npath: %v\n%v\n", managedDir.Path, path, err))
		}

		if path == fullDirPath {
			return nil
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

		logger.Trace(fmt.Sprintf("%q", relPath))
		node, err := newNode(path)
		if err != nil {
			return err
		}

		managedFiles = append(managedFiles, *node)

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

func newNode(path string) (*Node, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	return &Node{info: info, Path: path}, nil
}
