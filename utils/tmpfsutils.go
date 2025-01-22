package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type TmpFs struct {
	Dirs []*TmpDir
	Path string
}

type TmpDir struct {
	Name  string
	Dirs  []*TmpDir
	Files []*TmpFile
}

type TmpFile struct {
	Name    string
	Content []byte
}

func (f *TmpFs) Instantiate() error {
	var err error
	if f.Path == "" {
		f.Path, err = os.MkdirTemp("", "*")
		if err != nil {
			return err
		}
	} else {
		_, err := os.Lstat(f.Path)
		if err != nil {
			os.RemoveAll(f.Path)
		} else if os.IsNotExist(err) {
		} else {
			return err
		}
	}

	for _, tmpDir := range f.Dirs {
		err = tmpDir.Instantiate(f.Path)
		if err != nil {
			return errors.ErrUnsupported
		}
	}

	return nil
}

func (f *TmpFs) Destroy() error {
	return os.RemoveAll(filepath.Join(f.Path, ".."))
}

func (f *TmpFs) NodeCount() int {
	count := 0
	for _, dir := range f.Dirs {
		count += dir.NodeCount()
	}
	return count
}

func (d *TmpDir) Instantiate(parentPath string) error {
	if parentPath == "" {
		return errors.ErrUnsupported
	}

	path := filepath.Join(parentPath, d.Name)
	err := os.Mkdir(path, os.ModePerm)
	if err != nil {
		return err
	}

	// d.Path = path

	for _, tmpFile := range d.Files {
		err := tmpFile.Instantiate(path)
		if err != nil {
			return err
		}
	}

	for _, tmpDir := range d.Dirs {
		err := tmpDir.Instantiate(path)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *TmpDir) NodeCount() int {
	count := 0
	for _, subdir := range d.Dirs {
		count += subdir.NodeCount()
	}

	count += len(d.Files)
	return count
}

func (f *TmpFile) Instantiate(parentPath string) error {
	path := filepath.Join(parentPath, f.Name)

	// f.Path = path

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
