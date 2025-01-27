package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

type ActionKind int

const (
	TOUCH ActionKind = iota
	WRITE
	COPY
	REMOVE
	MOVE
	MKDIR
	RMDIR
)

type FsAction struct {
	Kind    ActionKind
	Content []byte
	SrcPath string
	DstPath string
}

func (a *FsAction) Take() error {
	switch a.Kind {
	case TOUCH:
		return touchFile(a.DstPath)
	case WRITE:
		return writeFile(a.DstPath, a.Content)
	case COPY:
		return copyFile(a.DstPath, a.SrcPath)
	case MOVE:
		return moveNode(a.DstPath, a.SrcPath)
	case REMOVE:
		return removeFile(a.DstPath)
	case MKDIR:
		return mkDir(a.DstPath)
	case RMDIR:
		return rmDir(a.DstPath)
	}

	return errors.New(fmt.Sprintf("unexpected utils.ActionKind: %#v", a.Kind))
}

// thanks https://golangbyexample.com/touch-file-golang/
func touchFile(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
	} else {
		now := time.Now().Local()
		err := os.Chtimes(path, now, now)
		if err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

func writeFile(path string, content []byte) error {
	f, err := openFileAppendOrCreate(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bytes.NewReader(content)
	if _, err := io.Copy(f, buf); err != nil {
		return err
	}
	return nil
}

func moveNode(dst string, src string) error {
	if err := os.Rename(src, dst); err != nil {
		return err
	}

	return nil
}

func copyFile(dst string, src string) error {
	_, err := os.Stat(src)
	if err != nil {
		return err
	}

	fsrc, err := os.Open(src)
	if err != nil {
		return err
	}
	defer fsrc.Close()

	fdst, err := openFileAppendOrCreate(dst)
	if err != nil {
		return err
	}
	defer fdst.Close()

	if _, err := io.Copy(fdst, fsrc); err != nil {
		return err
	}

	return nil
}

func removeFile(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}

	return nil
}

func mkDir(path string) error {
	if err := os.MkdirAll(path, 0770); err != nil {
		return err
	}
	return nil
}

func rmDir(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func openFileAppendOrCreate(path string) (*os.File, error) {
	var f *os.File
	var err error
	_, err = os.Stat(path)
	if err == nil { // existing file
		f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
	} else if os.IsNotExist(err) { // non-existent file
		f, err = os.Create(path)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	return f, nil
}
