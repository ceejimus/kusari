package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: add support for symlinks
type TmpDir struct {
	Top   bool
	Path  string
	Name  string
	Dirs  []*TmpDir
	Files []*TmpFile
}

type TmpFile struct {
	Name    string
	Path    string
	Content []byte
}

func (d *TmpDir) Instantiate(parentDir string) (string, error) {
	d.Top = false
	if parentDir == "" {
		tmpDir, err := os.MkdirTemp(parentDir, "*")
		if err != nil {
			return "", err
		}
		d.Top = true
		parentDir = tmpDir
	}

	path := filepath.Join(parentDir, d.Name)
	err := os.Mkdir(path, os.ModePerm)
	if err != nil {
		return "", err
	}

	d.Path = path

	for _, tmpFile := range d.Files {
		err := tmpFile.Instantiate(path)
		if err != nil {
			return "", err
		}
	}

	for _, tmpDir := range d.Dirs {
		_, err := tmpDir.Instantiate(path)
		if err != nil {
			return "", err
		}
	}

	return parentDir, nil
}

func (d *TmpDir) Destroy() error {
	if d.Top {
		return os.RemoveAll(filepath.Join(d.Path, ".."))
	}
	return nil
}

func (f *TmpFile) Instantiate(dir string) error {
	path := filepath.Join(dir, f.Name)

	f.Path = path

	handle, err := os.Create(path)
	if err != nil {
		return err
	}

	n, err := handle.Write(f.Content)
	if err != nil {
		return err
	}
	if n != int(f.Size()) {
		return errors.New(fmt.Sprintf("Failed to write TmpFile.Content to %q | %d/%d bytes written.\n", path, n, f.Size()))
	}

	return nil
}

func (f *TmpFile) Size() int64 {
	return int64(len(f.Content))
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	// shutdown()
	os.Exit(code)
}

func setup() {
	logLevel := os.Getenv("FILESERVER_LOG_LEVEL")
	if logLevel == "" {
		logLevel = "WARN"
	}
	myLogger, err := makeLogger(logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse create logger @ '%s'\n%s\n", "WARN", err)
		os.Exit(1)
	}
	logger = myLogger
}

func TestRelPathTrailingSlash(t *testing.T) {
	wanted := "subdir/f.txt"

	relDir := "/path/to/reldir/"
	fullPath := "/path/to/reldir/subdir/f.txt"

	got := relPath(fullPath, relDir)

	if wanted == got {
		return
	}

	assert.Equal(t, wanted, got)
}

func TestRelPathNoTrailingSlash(t *testing.T) {
	wanted := "subdir/f.txt"

	relDir := "/path/to/reldir"
	fullPath := "/path/to/reldir/subdir/f.txt"

	got := relPath(fullPath, relDir)

	if wanted == got {
		return
	}

	assert.Equal(t, wanted, got)
}

func TestFileHashWorks(t *testing.T) {
	content := "i am test"
	wanted := "97b74985df45e248be264fddc8172f71"

	tmpDir := TmpDir{
		Name: "d1",
		Dirs: make([]*TmpDir, 0),
		Files: []*TmpFile{
			{
				Name:    "f1.txt",
				Content: []byte(content),
			},
		},
	}
	_, err := tmpDir.Instantiate("")
	if err != nil {
		t.Error(err)
	}
	defer tmpDir.Destroy()

	got, err := fileHash(tmpDir.Files[0].Path)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, got, wanted)
}

func helperTestGetManagedFiles(t *testing.T, tmpDir TmpDir, include []string, exclude []string, wanted []string) {
	topdir, err := tmpDir.Instantiate("")
	if err != nil {
		t.Error(err)
	}
	defer tmpDir.Destroy()

	managedDir := ManagedDirectory{
		Path:    tmpDir.Name,
		Include: include,
		Exclude: exclude,
	}
	managedFiles, err := getManagedFiles(topdir, managedDir)

	got := make([]string, len(managedFiles))

	for i, managedFile := range managedFiles {
		got[i] = managedFile.Path
	}

	assert.ElementsMatch(t, wanted, got)
}

func TestGetManagedFilesEmptyDir(t *testing.T) {
	wanted := []string{}

	tmpDir := TmpDir{
		Name:  "d1",
		Dirs:  make([]*TmpDir, 0),
		Files: make([]*TmpFile, 0),
	}

	include := []string{}
	exclude := []string{}

	helperTestGetManagedFiles(t, tmpDir, include, exclude, wanted)
}

func TestGetManagedFiles(t *testing.T) {
	wanted := []string{
		"f1.txt",
		"f2.txt",
		"f3.txt",
	}

	tmpDir := TmpDir{
		Name: "d1",
		Dirs: make([]*TmpDir, 0),
		Files: []*TmpFile{
			{
				Name:    "f1.txt",
				Content: []byte("i am f1"),
			},
			{
				Name:    "f2.txt",
				Content: []byte("i am f2"),
			},
			{
				Name:    "f3.txt",
				Content: []byte("i am f3"),
			},
		},
	}

	include := []string{}
	exclude := []string{}

	helperTestGetManagedFiles(t, tmpDir, include, exclude, wanted)
}

func TestGetManagedFilesIncludeGlob(t *testing.T) {
	wanted := []string{
		"f1.txt",
	}

	tmpDir := TmpDir{
		Name: "d1",
		Dirs: make([]*TmpDir, 0),
		Files: []*TmpFile{
			{
				Name:    "f1.txt",
				Content: []byte("i am f1"),
			},
			{
				Name:    "bits.dat",
				Content: []byte("i am bits"),
			},
			{
				Name:    "bad.txt",
				Content: []byte("i am f3"),
			},
		},
	}

	include := []string{"f*.txt"}
	exclude := []string{}

	helperTestGetManagedFiles(t, tmpDir, include, exclude, wanted)
}

func TestGetManagedFilesExcludeGlob(t *testing.T) {
	wanted := []string{
		"f1.txt",
	}

	tmpDir := TmpDir{
		Name: "d1",
		Dirs: make([]*TmpDir, 0),
		Files: []*TmpFile{
			{
				Name:    "f1.txt",
				Content: []byte("i am f1"),
			},
			{
				Name:    "bits.dat",
				Content: []byte("i am bits"),
			},
			{
				Name:    "bad.txt",
				Content: []byte("i am f3"),
			},
		},
	}

	include := []string{}
	exclude := []string{"*.dat", "bad*"}

	helperTestGetManagedFiles(t, tmpDir, include, exclude, wanted)
}

func TestGetManagedFilesIncludeExcludeGlob(t *testing.T) {
	wanted := []string{
		"f1.txt",
	}

	tmpDir := TmpDir{
		Name: "d1",
		Dirs: make([]*TmpDir, 0),
		Files: []*TmpFile{
			{
				Name:    "f1.txt",
				Content: []byte("i am f1"),
			},
			{
				Name:    "f2.txt",
				Content: []byte("i am f2"),
			},
			{
				Name:    "bits.dat",
				Content: []byte("i am bits"),
			},
			{
				Name:    "bad.txt",
				Content: []byte("i am f3"),
			},
		},
	}

	include := []string{"f1*"}
	exclude := []string{"*.dat", "bad*"}

	helperTestGetManagedFiles(t, tmpDir, include, exclude, wanted)
}

func TestGetManagedFilesSubDirs(t *testing.T) {
	wanted := []string{
		"f1.txt",
		"f2.txt",
		"sub1/f1.txt",
		"sub2/f2.txt",
		"sub3/f3.txt",
		"sub3/sub4/f5.txt",
	}

	tmpDir := TmpDir{
		Name: "d1",
		Dirs: []*TmpDir{
			{
				Name: "sub1",
				Files: []*TmpFile{
					{
						Name:    "f1.txt",
						Content: []byte("i am sub1/f1"),
					},
				},
			},
			{
				Name: "sub2",
				Files: []*TmpFile{
					{
						Name:    "f2.txt",
						Content: []byte("i am sub2/f2"),
					},
				},
			},
			{
				Name: "sub3",
				Dirs: []*TmpDir{
					{
						Name: "sub4",
						Files: []*TmpFile{
							{
								Name:    "f5.txt",
								Content: []byte("i am sub5/f5"),
							},
						},
					},
				},
				Files: []*TmpFile{
					{
						Name:    "f3.txt",
						Content: []byte("i am sub3/f3"),
					},
				},
			},
		},
		Files: []*TmpFile{
			{
				Name:    "f1.txt",
				Content: []byte("i am f1"),
			},
			{
				Name:    "f2.txt",
				Content: []byte("i am f2"),
			},
		},
	}

	include := []string{}
	exclude := []string{}

	helperTestGetManagedFiles(t, tmpDir, include, exclude, wanted)
}

func TestGetManagedFilesSubDirsIncludeExcludeGlob(t *testing.T) {
	wanted := []string{
		"f1.txt",
		"f2.txt",
		"sub1/f1.txt",
		"sub3/f3.txt",
	}

	tmpDir := TmpDir{
		Name: "d1",
		Dirs: []*TmpDir{
			{
				Name: "sub1",
				Files: []*TmpFile{
					{
						Name:    "f1.txt",
						Content: []byte("i am sub1/f1"),
					},
				},
			},
			{
				Name: "sub2",
				Files: []*TmpFile{
					{
						Name:    "bits.dat",
						Content: []byte("1010011010"),
					},
				},
			},
			{
				Name: "sub3",
				Dirs: []*TmpDir{
					{
						Name: "sub4",
						Files: []*TmpFile{
							{
								Name:    "f5.txt",
								Content: []byte("i am sub5/f5"),
							},
						},
					},
				},
				Files: []*TmpFile{
					{
						Name:    "f3.txt",
						Content: []byte("i am sub3/f3"),
					},
				},
			},
		},
		Files: []*TmpFile{
			{
				Name:    "f1.txt",
				Content: []byte("i am f1"),
			},
			{
				Name:    "f2.txt",
				Content: []byte("i am f2"),
			},
		},
	}

	include := []string{"*.txt"}
	exclude := []string{"sub3/sub4**"}

	helperTestGetManagedFiles(t, tmpDir, include, exclude, wanted)
}
