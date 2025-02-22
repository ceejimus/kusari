package fnode

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/ceejimus/kusari/logger"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	// shutdown()
	os.Exit(code)
}

func setup() {
	logger.Init("")
}

func TestRelPathTrailingSlash(t *testing.T) {
	wanted := "subdir/f.txt"

	relDir := "/path/to/reldir/"
	fullPath := "/path/to/reldir/subdir/f.txt"

	got := GetRelativePath(fullPath, relDir)

	if wanted == got {
		return
	}

	assert.Equal(t, wanted, got)
}

func TestRelPathNoTrailingSlash(t *testing.T) {
	wanted := "subdir/f.txt"

	relDir := "/path/to/reldir"
	fullPath := "/path/to/reldir/subdir/f.txt"

	got := GetRelativePath(fullPath, relDir)

	if wanted == got {
		return
	}

	assert.Equal(t, wanted, got)
}

func TestFileHashWorks(t *testing.T) {
	content := "i am test"
	wanted, err := GetHash(bytes.NewReader([]byte(content)))
	if err != nil {
		t.Error(err)
	}

	dir := os.TempDir()
	path := filepath.Join(dir, "tmpfile")
	file, err := os.Create(path)
	if err != nil {
		t.Error(err.Error())
	}
	file.Write([]byte(content))
	file.Close()

	got, err := FileHash(path)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, got, wanted)
}
