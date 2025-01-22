package syncd

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Dir struct {
	ID   uuid.UUID // should be generated when adding
	Path string    // relative to configured top-level directory
}

type Chain struct {
	ID uuid.UUID // should be generated when adding
	// DirID uuid.UUID // ID of chain's directory
	Ino uint64 // the local inode
	// HeadID uuid.UUID // the first event in the chain
	// TailID uuid.UUID // the last event in the chain
}

type Event struct {
	ID uuid.UUID // should be generated when adding
	// DirID     uuid.UUID // ID of managed dir
	Timestamp time.Time // timestamp of the event
	Path      string    // relative to DirName
	Type      string    // create, remove, rename, write, chmod
	Size      uint64    // file size
	Hash      *string   // file hash (null for non-files)
	ModTime   time.Time // modification time
	// PrevID    uuid.UUID // the previous event ID
	// NextID    uuid.UUID // the next event ID
}

type EventStore interface {
	AddDir(dir Dir) (*Dir, error)                               // add a new syncd directory
	AddChain(chain Chain, dirID uuid.UUID) (*Chain, error)      // add a new event chain
	AddEvent(event Event, chainID uuid.UUID) (*Event, error)    // add a new event
	GetDirByUUID(id uuid.UUID) (*Dir, bool)                     // get syncd directory by UUID
	GetDirByPath(path string) (*Dir, bool)                      // get syncd dir by path
	GetChainByUUID(id uuid.UUID) (*Chain, bool)                 // get chain by UUID
	GetChainByPath(dirID uuid.UUID, path string) (*Chain, bool) // get chain whose tail is event w/ path
	GetChainByIno(ino uint64) (*Chain, bool)                    // get chain by ino
	GetEventByUUID(id uuid.UUID) (*Event, bool)                 // get event by UUID
	GetChainsInDir(id uuid.UUID) ([]Chain, bool)
	GetEventsInChain(id uuid.UUID) ([]Event, bool)
	// SetChainTail(id uuid.UUID, eventID uuid.UUID) error          // set new tail event for chain
	// SetEventNext(id uuid.UUID, eventID uuid.UUID) error          // set new tail event for chain
}

func (d Dir) String() string {
	return fmt.Sprintf("Dir: %s %q", d.ID, d.Path)
}

func (c Chain) String() string {
	return fmt.Sprintf("Chain: %s <%d>", c.ID, c.Ino)
}

func (e Event) String() string {
	hash := "<nil>"
	if e.Hash != nil {
		hash = *e.Hash
	}
	return fmt.Sprintf("Event: (%s) %s - %s %q |%s|",
		e.Type,
		e.Timestamp.Format(time.RFC1123),
		e.ID,
		e.Path,
		hash,
	)
}
