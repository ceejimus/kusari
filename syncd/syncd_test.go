package syncd

import (
	"path/filepath"
	"testing"

	"atmoscape.net/fileserver/fnode"
	"atmoscape.net/fileserver/utils"
	"github.com/stretchr/testify/assert"
)

func helperTestGetManagedFiles(t *testing.T, tmpDir utils.TmpDir, include []string, exclude []string, wanted []string) {
	tmpFs := utils.TmpFs{Dirs: []*utils.TmpDir{&tmpDir}}
	err := tmpFs.Instantiate()
	if err != nil {
		t.Error(err)
	}
	defer tmpFs.Destroy()

	managedDir := ManagedDirectory{
		Path:    tmpDir.Name,
		Include: include,
		Exclude: exclude,
	}
	managedFiles, err := GetManagedNodes(tmpFs.Path, managedDir)

	got := make([]string, len(managedFiles))

	for i, managedFile := range managedFiles {
		got[i] = fnode.GetRelativePath(managedFile.Path, filepath.Join(tmpFs.Path, managedDir.Path))
	}

	assert.ElementsMatch(t, wanted, got)
}

func TestGetManagedFilesEmptyDir(t *testing.T) {
	wanted := []string{}

	tmpDir := utils.TmpDir{
		Name:  "d1",
		Dirs:  make([]*utils.TmpDir, 0),
		Files: make([]*utils.TmpFile, 0),
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

	tmpDir := utils.TmpDir{
		Name: "d1",
		Dirs: make([]*utils.TmpDir, 0),
		Files: []*utils.TmpFile{
			{Name: "f1.txt", Content: []byte("i am f1")},
			{Name: "f2.txt", Content: []byte("i am f2")},
			{Name: "f3.txt", Content: []byte("i am f3")},
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

	tmpDir := utils.TmpDir{
		Name: "d1",
		Dirs: make([]*utils.TmpDir, 0),
		Files: []*utils.TmpFile{
			{Name: "f1.txt", Content: []byte("i am f1")},
			{Name: "bits.dat", Content: []byte("i am bits")},
			{Name: "bad.txt", Content: []byte("i am f3")},
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

	tmpDir := utils.TmpDir{
		Name: "d1",
		Dirs: make([]*utils.TmpDir, 0),
		Files: []*utils.TmpFile{
			{Name: "f1.txt", Content: []byte("i am f1")},
			{Name: "bits.dat", Content: []byte("i am bits")},
			{Name: "bad.txt", Content: []byte("i am f3")},
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

	tmpDir := utils.TmpDir{
		Name: "d1",
		Dirs: make([]*utils.TmpDir, 0),
		Files: []*utils.TmpFile{
			{Name: "f1.txt", Content: []byte("i am f1")},
			{Name: "f2.txt", Content: []byte("i am f2")},
			{Name: "bits.dat", Content: []byte("i am bits")},
			{Name: "bad.txt", Content: []byte("i am f3")},
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
		"sub1",
		"sub2",
		"sub3",
		"sub3/sub4",
	}

	tmpDir := utils.TmpDir{
		Name: "d1",
		Dirs: []*utils.TmpDir{
			{
				Name: "sub1",
				Files: []*utils.TmpFile{
					{Name: "f1.txt", Content: []byte("i am sub1/f1")},
				},
			},
			{
				Name: "sub2",
				Files: []*utils.TmpFile{
					{Name: "f2.txt", Content: []byte("i am sub2/f2")},
				},
			},
			{
				Name: "sub3",
				Dirs: []*utils.TmpDir{
					{
						Name: "sub4",
						Files: []*utils.TmpFile{
							{Name: "f5.txt", Content: []byte("i am sub5/f5")},
						},
					},
				},
				Files: []*utils.TmpFile{
					{Name: "f3.txt", Content: []byte("i am sub3/f3")},
				},
			},
		},
		Files: []*utils.TmpFile{
			{Name: "f1.txt", Content: []byte("i am f1")},
			{Name: "f2.txt", Content: []byte("i am f2")},
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
		"sub1",
		"sub2",
		"sub3",
	}

	tmpDir := utils.TmpDir{
		Name: "d1",
		Dirs: []*utils.TmpDir{
			{
				Name: "sub1",
				Files: []*utils.TmpFile{
					{Name: "f1.txt", Content: []byte("i am sub1/f1")},
				},
			},
			{
				Name: "sub2", Files: []*utils.TmpFile{
					{Name: "bits.dat", Content: []byte("1010011010")},
				},
			},
			{
				Name: "sub3",
				Dirs: []*utils.TmpDir{
					{
						Name: "sub4",
						Files: []*utils.TmpFile{
							{Name: "f5.txt", Content: []byte("i am sub5/f5")},
						},
					},
				},
				Files: []*utils.TmpFile{
					{Name: "f3.txt", Content: []byte("i am sub3/f3")},
				},
			},
		},
		Files: []*utils.TmpFile{
			{Name: "f1.txt", Content: []byte("i am f1")},
			{Name: "f2.txt", Content: []byte("i am f2")},
		},
	}

	include := []string{"*.txt", "**/"}
	exclude := []string{"sub3/sub4**"}

	helperTestGetManagedFiles(t, tmpDir, include, exclude, wanted)
}
