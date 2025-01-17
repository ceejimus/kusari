package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"io/fs"
)

type NodeType int

const (
	FILE NodeType = iota + 1
	DIR
)

type NodeState struct {
	Path    string
	ModTime time.Time
	Hash    *string
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

func (n *Node) ModTime() time.Time {
	return n.info.ModTime()
}

func (n *Node) Hash() (*string, error) {
	if n.Type() != FILE {
		return nil, errors.ErrUnsupported
	}

	hash, err := FileHash(n.Path)
	if err != nil {
		return nil, err
	}

	return &hash, nil
}

func (n *Node) State() NodeState {
	hash, _ := n.Hash()
	return NodeState{
		Path:    n.Path,
		ModTime: n.ModTime(),
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
	b.WriteString(fmt.Sprintf(" %s", n.ModTime().Format("2006-01-02 15:04:05 MST")))
	b.WriteString(fmt.Sprintf(" %s", n.info.Name()))
	hash, err := n.Hash()
	if err == nil {
		b.WriteString(fmt.Sprintf(" |%s|", *hash))
	}
	return b.String()
}

func (n *NodeState) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%.8d", n.Size))
	//	01/02 03:04:05PM '06 -0700
	//	Mon Jan 2 15:04:05 MST 2006
	b.WriteString(fmt.Sprintf(" %s", n.ModTime.Format("2006-01-02 15:04:05 MST")))
	b.WriteString(fmt.Sprintf(" %s", filepath.Base(n.Path)))
	if n.Hash != nil {
		b.WriteString(fmt.Sprintf(" |%s|", *n.Hash))
	}
	return b.String()
}

func NewNode(path string) (*Node, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	return &Node{info: info, Path: path}, nil
}
