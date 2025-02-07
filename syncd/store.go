// Event persistence layer and object model
//
// All Events happen within a pre-configured top-level directory (Dir).
// These events are Chained together to represent the evolution of a particular
// node (inode) on the filesystem.
// To this end: Events go on Chains go in Dirs; Dirs <- Chains <- Events

package syncd

import (
	"fmt"
	"time"
)

// Internal "Op" enum (Create, Write, etc.)
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

type ID interface {
	Encode() []byte // encode ID to bytes
}

// top-level directory
type Dir struct {
	ID   ID     // should be generated when adding
	Path string // relative to configured top-level directory
}

// chain of events for a given inode
type Chain struct {
	ID  ID     // should be generated when adding
	Ino uint64 // the local inode
}

// something that happened to a node
type Event struct {
	ID        ID        // should be generated when adding
	Timestamp time.Time // timestamp of the event
	Path      string    // relative to DirName
	Type      EventType // create, remove, rename, write, chmod
	Size      uint64    // file size
	Hash      *string   // file hash (null for non-files)
	ModTime   time.Time // modification time
}

// EventStore is the interface that persists dirs, chains and events
//
// NOTE: this interface is a WIP, it doesn't include delete definitions for one
// it uses []byte for ID for another
//
// The AddX methods add new objects to database.
// These methods should generate and set the ID on the object (modify-in-place).
// If these methods receive an object w/ a non nil ID then they should error
// Can error for reasons specific to the object being persisted (see below)
// or for any other reason the implementation dictates.
//
// The GetXByY methods retrieve objects from database.
// These methods should not error unless something "bad" happens.
// Not finding the specified object is not "bad";
// It is thus up to the caller to check if the returned pointer is nil
// in addition to checking the error.
type EventStore interface {
	// add a new directory
	// should error if user tries to add dir w/ existing path
	AddDir(dir *Dir) error
	// add a new event chain
	// should error if user specifies dirID of nonexistent Dir
	AddChain(chain *Chain, dirID ID) error
	// add a new event
	// should error if user specifies chainID of nonexistent Chain
	AddEvent(event *Event, chainID ID) error
	// get syncd directory by ID
	GetDirByID(dirID ID) (*Dir, error)
	// get syncd dir by path
	GetDirByPath(path string) (*Dir, error)
	// get chain by ID
	GetChainByID(chainID ID) (*Chain, error)
	// get chain whose tail is event w/ path
	// be careful implementing this one; it should return the chain whose most
	// recent event was for an inode w/ the given path
	//
	// for example, if subdir "s1" containing file "f" (s1/f) is renamed to "s2/f"
	// then GetChainByPath(..., "s2/f") should return the chain tracking that node
	// and GetChainByPath(..., "s1/f") should return nil
	//
	// NOTE: after a node is removed, this should return nil
	// until a new node re-uses the name
	GetChainByPath(dirID ID, path string) (*Chain, error)
	// get chain by ino
	// like the above, after a node is removed, this should return nil
	// until a new node re-uses the inode
	GetChainByIno(ino uint64) (*Chain, error)
	// get event by ID
	GetEventByID(eventID ID) (*Event, error)
	// get all stored dirs
	GetDirs() ([]Dir, error)
	// get all chains in directory
	GetChainsInDir(dirID ID) ([]Chain, error)
	// get all events in chain
	GetEventsInChain(chainId ID) ([]Event, error)
	// something for owners to call to cleanup underlying resources
	Close() error
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
