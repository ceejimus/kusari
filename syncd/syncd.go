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

type SyncdDirectory struct {
	Path    string   `yaml:"path"`
	Include []string `yaml:"incl"`
	Exclude []string `yaml:"excl"`
}

func GetSyncdNodes(topDir string, syncdDir SyncdDirectory) ([]fnode.Node, error) {
	syncdNodes := make([]fnode.Node, 0)

	inclGlobs := mapToGlobs(syncdDir.Include)
	exclGlobs := mapToGlobs(syncdDir.Exclude)

	fullDirPath := filepath.Join(topDir, syncdDir.Path)

	logger.Trace(fmt.Sprintf("Walking syncd dir topdir: %q - syncdDir.Path: %q - %q\n", topDir, syncdDir.Path, fullDirPath))
	err := filepath.WalkDir(fullDirPath, func(path string, d fs.DirEntry, err error) error {

		if err != nil {
			return errors.New(fmt.Sprintf("Failure when walking dir: %v\npath: %v\n%v\n", syncdDir.Path, path, err))
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

		syncdNodes = append(syncdNodes, *node)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return syncdNodes, nil
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
