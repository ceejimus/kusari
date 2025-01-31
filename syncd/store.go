package syncd

import (
	"fmt"
	"time"
)

// Internal "Op" enum
type EventType uint32

// fsnotify events are processed to internal structures
// for now we only care about a few
const (
	// new file created
	Create EventType = 1 << iota

	// bytes written to file (or truncation)
	Write

	// file deleted
	Remove

	// file moved
	Rename

	// various events including attribute change and file writes
	Chmod
)

type Dir struct {
	ID   []byte // should be generated when adding
	Path string // relative to configured top-level directory
}

type Chain struct {
	ID  []byte // should be generated when adding
	Ino uint64 // the local inode
}

type Event struct {
	ID        []byte    // should be generated when adding
	Timestamp time.Time // timestamp of the event
	Path      string    // relative to DirName
	Type      EventType // create, remove, rename, write, chmod
	Size      uint64    // file size
	Hash      *string   // file hash (null for non-files)
	ModTime   time.Time // modification time
}

type EventStore interface {
	AddDir(dir Dir) (*Dir, error)                            // add a new syncd directory
	AddChain(chain Chain, dirID []byte) (*Chain, error)      // add a new event chain
	AddEvent(event Event, chainID []byte) (*Event, error)    // add a new event
	GetDirByID(id []byte) (*Dir, bool)                       // get syncd directory by ID
	GetDirByPath(path string) (*Dir, bool)                   // get syncd dir by path
	GetChainByID(id []byte) (*Chain, bool)                   // get chain by ID
	GetChainByPath(dirID []byte, path string) (*Chain, bool) // get chain whose tail is event w/ path
	GetChainByIno(ino uint64) (*Chain, bool)                 // get chain by ino
	GetEventByID(id []byte) (*Event, bool)                   // get event by ID
	GetDirs() []Dir                                          // get all stored dirs
	GetChainsInDir(id []byte) ([]Chain, bool)                // get all chains in directory w/ ID
	GetEventsInChain(id []byte) ([]Event, bool)              // get all events in chain w/ ID
	Close() error                                            // something for owners to call to cleanup underlying resources
}

func (d Dir) String() string {
	return fmt.Sprintf("Dir: %s %q", d.ID, d.Path)
}

func (c Chain) String() string {
	return fmt.Sprintf("Chain: %s <%d>", c.ID, c.Ino)
}

func (e Event) String() string {
	hash := ""
	if e.Hash != nil {
		hash = *e.Hash
	}
	return fmt.Sprintf("(<%s> %s %d:|%s|)",
		e.Type,
		e.Path,
		e.Size,
		hash,
	)
}
