package utils

import (
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
		return moveFile(a.DstPath, a.SrcPath)
	case REMOVE:
		return removeFile(a.SrcPath)
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
	var f *os.File
	var err error
	_, err = os.Stat(path)
	if err == nil { // existing file
		f, err = os.Open(path)
		if err != nil {
			return err
		}
	} else if os.IsNotExist(err) { // non-existent file
		f, err = os.Create(path)
		if err != nil {
			return err
		}
	} else {
		return err
	}
	defer f.Close()

	for {
		n, err := f.Write(content)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		content = content[n:]
	}
}

func moveFile(dst string, src string) error {
	if err := copyFile(dst, src); err != nil {
		return err
	}

	if err := removeFile(src); err != nil {
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

	fdst, err := os.Create(dst)
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
