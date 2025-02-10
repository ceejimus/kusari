package test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/ceejimus/kusari/fnode"
	"github.com/ceejimus/kusari/logger"
	"github.com/ceejimus/kusari/syncd"
	"github.com/ceejimus/kusari/utils"
	"golang.org/x/sys/unix"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	// shutdown()
	os.Exit(code)
}

var DIR_BLOCK_SIZE uint64

func setup() {
	logger.Init("")
	wd, err := os.Getwd()
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to get wd %s", err))
		os.Exit(-1)
	}
	var statfs unix.Statfs_t
	err = unix.Statfs(wd, &statfs)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to stat wd %q: %s", wd, err))
		os.Exit(-1)
	}
	DIR_BLOCK_SIZE = uint64(statfs.Bsize)
}

// test a remove for a single file
func TestExistingFileRemove(t *testing.T) {
	content := []byte("i am a")
	hash, err := fnode.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
		Files: []*utils.TmpFile{
			{Name: "a", Content: content},
		},
	}

	tmpFs := utils.TmpFs{Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.REMOVE, DstPath: "d/a"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test a create, write, rename, remove flow for single file
func TestSingleFileCWRR(t *testing.T) {
	content := []byte("I am Weasel!")
	hash, err := fnode.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["b"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Write,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "a",
				Type: syncd.Rename,
			},
			{
				Path: "b",
				Type: syncd.Create,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "b",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.WRITE, Content: content, DstPath: "d/a"},
		{Kind: utils.MOVE, SrcPath: "d/a", DstPath: "d/b"},
		{Kind: utils.REMOVE, DstPath: "d/b"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test creating a file w/ same old path as renamed file
func TestTouchMovedFilename(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["b"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Rename,
			},
			{
				Path: "b",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "b",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.MOVE, SrcPath: "d/a", DstPath: "d/b"},
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/b"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test moving files between subdirs
func TestFilesMovedBetweenSubdirs(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2"] = Chains{
		Chain{
			{
				Path: "s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/a"] = Chains{
		Chain{
			{
				Path: "s1/a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "s1/a",
				Type: syncd.Rename,
			},
			{
				Path: "s2/a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "s2/a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1"},
		{Kind: utils.MKDIR, DstPath: "d/s2"},
		{Kind: utils.TOUCH, DstPath: "d/s1/a"},
		{Kind: utils.MOVE, SrcPath: "d/s1/a", DstPath: "d/s2/a"},
		{Kind: utils.REMOVE, DstPath: "d/s2/a"},
		{Kind: utils.RMDIR, DstPath: "d/s1"},
		{Kind: utils.RMDIR, DstPath: "d/s2"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test moving/renaming a subdir w/ files
func TestEmptyDirsMovedBetweenSubdirs(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2"] = Chains{
		Chain{
			{
				Path: "s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s3"] = Chains{
		Chain{
			{
				Path: "s1/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1/s3",
				Type: syncd.Rename,
			},
			{
				Path: "s2/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2/s3",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1"},
		{Kind: utils.MKDIR, DstPath: "d/s2"},
		{Kind: utils.MKDIR, DstPath: "d/s1/s3"},
		{Kind: utils.MOVE, SrcPath: "d/s1/s3", DstPath: "d/s2/s3"},
		{Kind: utils.RMDIR, DstPath: "d/s2/s3"},
		{Kind: utils.RMDIR, DstPath: "d/s1"},
		{Kind: utils.RMDIR, DstPath: "d/s2"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test moving/renaming a subdir w/ files
func TestNonEmptyDirsMovedBetweenSubdirs(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2"] = Chains{
		Chain{
			{
				Path: "s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s3"] = Chains{
		Chain{
			{
				Path: "s1/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1/s3",
				Type: syncd.Rename,
			},
			{
				Path: "s2/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2/s3",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s3/a"] = Chains{
		Chain{
			{
				Path: "s1/s3/a",
				Type: syncd.Create,
			},
			{
				Path: "s2/s3/a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1"},
		{Kind: utils.MKDIR, DstPath: "d/s2"},
		{Kind: utils.MKDIR, DstPath: "d/s1/s3"},
		{Kind: utils.TOUCH, DstPath: "d/s1/s3/a"},
		{Kind: utils.MOVE, SrcPath: "d/s1/s3", DstPath: "d/s2/s3"},
		{Kind: utils.REMOVE, DstPath: "d/s2/s3/a"},
		{Kind: utils.RMDIR, DstPath: "d/s2/s3"},
		{Kind: utils.RMDIR, DstPath: "d/s1"},
		{Kind: utils.RMDIR, DstPath: "d/s2"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test moving/renaming a subdir w/ files
func TestMoveNonEmptySubdirThenMoveFileOut(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2"] = Chains{
		Chain{
			{
				Path: "s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s4"] = Chains{
		Chain{
			{
				Path: "s2/s4",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2/s4",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s2/s4/s3"] = Chains{
		Chain{
			{
				Path: "s1/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s1/s3",
				Type: syncd.Rename,
			},
			{
				Path: "s2/s4/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
			{
				Path: "s2/s4/s3",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["s1/a"] = Chains{
		Chain{
			{
				Path: "s1/s3/a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "s2/s4/s3/a",
				Type: syncd.Rename,
			},
			{
				Path: "s1/a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "s1/a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1"},
		{Kind: utils.MKDIR, DstPath: "d/s2"},
		{Kind: utils.MKDIR, DstPath: "d/s1/s3"},
		{Kind: utils.TOUCH, DstPath: "d/s1/s3/a"},
		{Kind: utils.MKDIR, DstPath: "d/s2/s4"},
		{Kind: utils.MOVE, SrcPath: "d/s1/s3", DstPath: "d/s2/s4/s3"},
		{Kind: utils.MOVE, SrcPath: "d/s2/s4/s3/a", DstPath: "d/s1/a"},
		{Kind: utils.REMOVE, DstPath: "d/s1/a"},
		{Kind: utils.RMDIR, DstPath: "d/s2/s4/s3"},
		{Kind: utils.RMDIR, DstPath: "d/s2/s4"},
		{Kind: utils.RMDIR, DstPath: "d/s2"},
		{Kind: utils.RMDIR, DstPath: "d/s1"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test reusing a removed filename
func TestInodeReuseByTouch(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test reusing a removed filename by existing file
func TestFileReuseByMove(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
		},
		Chain{
			{
				Path: "b",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "b",
				Type: syncd.Rename,
			},
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.TOUCH, DstPath: "d/b"},
		{Kind: utils.MOVE, SrcPath: "d/b", DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test file copied then removed
func TestFileCopiedThenRemoved(t *testing.T) {
	content := []byte("I am Weasel!")
	hash, err := fnode.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["a"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Write,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "a",
				Type: syncd.Remove,
			},
		},
	}
	wantedMap["d"]["b"] = Chains{
		Chain{
			{
				Path: "b",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "b",
				Type: syncd.Write,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "b",
				Type: syncd.Remove,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d/a"},
		{Kind: utils.WRITE, DstPath: "d/a", Content: content},
		{Kind: utils.COPY, DstPath: "d/b", SrcPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/a"},
		{Kind: utils.REMOVE, DstPath: "d/b"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test making directories recursively
func TestNestedMkdir(t *testing.T) {
	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
		},
	}
	wantedMap["d"]["s1/s2"] = Chains{
		Chain{
			{
				Path: "s1/s2",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
		},
	}
	wantedMap["d"]["s1/s2/s3"] = Chains{
		Chain{
			{
				Path: "s1/s2/s3",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
		},
	}

	tmpDir := utils.TmpDir{
		Name: "d",
		Dirs: make([]*utils.TmpDir, 0),
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&tmpDir}}

	actions := []utils.FsAction{
		{Kind: utils.MKDIR, DstPath: "d/s1/s2/s3"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test create event for content of subdirs moved into watched dir
func TestMoveDirWContent(t *testing.T) {
	content := []byte("i am a")
	hash, err := fnode.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(DirPathToTailChainMap)
	wantedMap["d"] = make(TailPathToChainMap)
	wantedMap["d"]["s1"] = Chains{
		Chain{
			{
				Path: "s1",
				Type: syncd.Create,
				Size: DIR_BLOCK_SIZE,
			},
		},
	}
	wantedMap["d"]["s1/a"] = Chains{
		Chain{
			{
				Path: "s1/a",
				Type: syncd.Create,
				Size: uint64(len(content)),
				Hash: &hash,
			},
		},
	}

	watchedDir := utils.TmpDir{Name: "d"}

	contentDir := utils.TmpDir{
		Name: "src",
		Dirs: []*utils.TmpDir{{
			Name: "s1",
			Files: []*utils.TmpFile{{
				Name:    "a",
				Content: content,
			}}}},
	}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&watchedDir, &contentDir}}

	actions := []utils.FsAction{
		{Kind: utils.MOVE, SrcPath: "src/s1", DstPath: "d/s1"},
	}

	runApiTest(t, &tmpFs, []string{"d"}, actions, wantedMap)
}

// test create event for content of subdirs moved into watched dir
func TestMultipleWatchedDirs(t *testing.T) {
	content := []byte("i am a")
	hash, err := fnode.GetHash(bytes.NewBuffer(content))
	if err != nil {
		t.Fatal(err)
	}

	wantedMap := make(DirPathToTailChainMap)

	wantedMap["d1"] = make(TailPathToChainMap)
	wantedMap["d1"]["b"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Write,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "a",
				Type: syncd.Rename,
			},
			{
				Path: "b",
				Type: syncd.Create,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "b",
				Type: syncd.Remove,
			},
		},
	}

	wantedMap["d2"] = make(TailPathToChainMap)
	wantedMap["d2"]["b"] = Chains{
		Chain{
			{
				Path: "a",
				Type: syncd.Create,
				Size: 0,
			},
			{
				Path: "a",
				Type: syncd.Write,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "a",
				Type: syncd.Rename,
			},
			{
				Path: "b",
				Type: syncd.Create,
				Size: uint64(len(content)),
				Hash: &hash,
			},
			{
				Path: "b",
				Type: syncd.Remove,
			},
		},
	}

	watchedDir1 := utils.TmpDir{Name: "d1"}
	watchedDir2 := utils.TmpDir{Name: "d2"}

	tmpFs := utils.TmpFs{Path: "../.data/tmp/", Dirs: []*utils.TmpDir{&watchedDir1, &watchedDir2}}

	actions := []utils.FsAction{
		{Kind: utils.TOUCH, DstPath: "d1/a"},
		{Kind: utils.WRITE, Content: content, DstPath: "d1/a"},
		{Kind: utils.MOVE, SrcPath: "d1/a", DstPath: "d1/b"},
		{Kind: utils.REMOVE, DstPath: "d1/b"},
		{Kind: utils.TOUCH, DstPath: "d2/a"},
		{Kind: utils.WRITE, Content: content, DstPath: "d2/a"},
		{Kind: utils.MOVE, SrcPath: "d2/a", DstPath: "d2/b"},
		{Kind: utils.REMOVE, DstPath: "d2/b"},
	}

	runApiTest(t, &tmpFs, []string{"d1", "d2"}, actions, wantedMap)
}
