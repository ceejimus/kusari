package syncd

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"atmoscape.net/fileserver/fnode"
	"atmoscape.net/fileserver/logger"
	"github.com/gobwas/glob"
)

type ManagedDirectory struct {
	Path    string   `yaml:"path"`
	Include []string `yaml:"incl"`
	Exclude []string `yaml:"excl"`
}

type ManagedMap map[string][]fnode.Node

func GetManagedMap(topDir string, managedDirs []ManagedDirectory) (ManagedMap, error) {
	managedMap := make(map[string][]fnode.Node)

	for _, managedDir := range managedDirs {
		managedFiles, err := GetManagedNodes(topDir, managedDir)
		if err != nil {
			return nil, err
		}

		managedMap[managedDir.Path] = managedFiles
	}

	return managedMap, nil
}

func GetManagedNodes(topDir string, managedDir ManagedDirectory) ([]fnode.Node, error) {
	managedFiles := make([]fnode.Node, 0)

	inclGlobs := mapToGlobs(managedDir.Include)
	exclGlobs := mapToGlobs(managedDir.Exclude)

	fullDirPath := filepath.Join(topDir, managedDir.Path)

	logger.Trace(fmt.Sprintf("Walking managed dir topdir: %q - managedDir.Path: %q - %q\n", topDir, managedDir.Path, fullDirPath))
	err := filepath.WalkDir(fullDirPath, func(path string, d fs.DirEntry, err error) error {

		if err != nil {
			return errors.New(fmt.Sprintf("Failure when walking dir: %v\npath: %v\n%v\n", managedDir.Path, path, err))
		}

		if path == fullDirPath {
			return nil
		}

		relPath := fnode.GetRelativePath(path, fullDirPath)
		// add trailing slash to directories so we can match on our directory globs
		if d.Type().IsDir() && !strings.HasSuffix(relPath, "/") {
			relPath = fmt.Sprintf("%s/", relPath)
		}

		// for now we only check globs on fnode
		if checkGlobs(exclGlobs, relPath, false) {
			logger.Trace(fmt.Sprintf("Excluded - %v : %v", relPath, d))
			return nil
		}

		if !checkGlobs(inclGlobs, relPath, true) {
			logger.Trace(fmt.Sprintf("Not included - %v : %v", relPath, d))
			return nil
		}

		// we're only going to look at regular fnode and regular dirs
		// TODO: implement our own directory recursion?
		if !d.Type().IsRegular() && !d.Type().IsDir() {
			logger.Trace(fmt.Sprintf("SKIPPING - %v : %v", relPath, d))
			return nil
		}

		logger.Trace(fmt.Sprintf("Adding - %v : %v", relPath, d))

		logger.Trace(fmt.Sprintf("%q", relPath))
		node, err := fnode.NewNode(path)
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
