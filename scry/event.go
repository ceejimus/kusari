package scry

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ceejimus/kusari/fnode"
	"github.com/ceejimus/kusari/logger"
	"github.com/fsnotify/fsnotify"
)

// TODO: make "enum"
type NodeEvent struct {
	Type      EventType        // "create", "modify", "delete", "rename"
	FullPath  string           // the name of the underlying event (full path)
	Path      string           // path of node relative to Dir.Path
	OldPath   *string          // set for the "create" event in rename pair
	Timestamp time.Time        // time event processed
	State     *fnode.NodeState // node state pointer
	doneTime  time.Time        // time the event finished processing
	node      *fnode.Node      // node pointer
	dir       *Dir             // stored Dir for this event
	chain     *Chain           // stored Chain for this event
}

func (t EventType) String() string {
	switch t {
	case Chmod:
		return "chmod"
	case Create:
		return "create"
	case Remove:
		return "remove"
	case Rename:
		return "rename"
	case Write:
		return "write"
	default:
		panic(fmt.Sprintf("unexpected scry.NodeEventType: %#v", t))
	}
}

func (n NodeEvent) ProcessingTime() time.Duration {
	if time.Time.IsZero(n.doneTime) {
		return time.Duration(0)
	}
	return n.doneTime.Sub(n.Timestamp)
}

// process and store NodeEvent
func processNodeEvent(nodeEvent *NodeEvent, store EventStore) error {
	var err error
	// set node info on event
	if err = setNode(nodeEvent); err != nil {
		return errors.New(fmt.Sprintf("Failed to set node for: %+v", *nodeEvent))
	}
	// lookup chain for this event
	if err = lkpChain(nodeEvent, store); err != nil {
		return errors.New(fmt.Sprintf("Failed to find lookup Chain for event:  %v", nodeEvent))
	}
	// ignore invalid events for now
	if err := isValidEvent(nodeEvent); err != nil {
		return err
	}
	// add new chain for new nodes
	if nodeEvent.Type == Create && nodeEvent.chain == nil {
		// create new chain
		newChain := &Chain{Ino: nodeEvent.node.Ino}
		// create and new chain
		if err = store.AddChain(newChain, nodeEvent.dir.ID); err != nil {
			logger.Fatal(err.Error())
			os.Exit(1)
		}
		nodeEvent.chain = newChain
	}
	// create new event to store
	event := &Event{
		Timestamp: nodeEvent.Timestamp,
		Path:      nodeEvent.Path,
		Type:      nodeEvent.Type,
	}
	// set event state from node
	setEventState(event, nodeEvent.node)
	// add event to store
	if err = store.AddEvent(event, nodeEvent.chain.ID); err != nil {
		logger.Fatal(err.Error())
		os.Exit(1)
	}
	// set old path
	nodeEvent.OldPath = event.OldPath
	return nil
}

// toNodeEvent constructs a NodeEvent from an fsnotify.Event
// it add a timestamp and returns nil if this isn't an event we care about
func toNodeEvent(event *fsnotify.Event) *NodeEvent {
	// init nodeEvent
	nodeEvent := NodeEvent{
		FullPath:  event.Name,
		Timestamp: time.Now(),
	}
	// set internal type
	switch event.Op {
	case fsnotify.Create:
		nodeEvent.Type = Create
	case fsnotify.Write:
		nodeEvent.Type = Write
	case fsnotify.Remove:
		nodeEvent.Type = Remove
	case fsnotify.Rename:
		nodeEvent.Type = Rename
	default:
		return nil
	}

	return &nodeEvent
}

// setNode sets the node property on the NodeEvent struct
// the node is used to: detect new dirs, lookup chains by ino, set event state
func setNode(nodeEvent *NodeEvent) error {
	switch nodeEvent.Type {
	case Create, Write:
		node, err := fnode.NewNode(nodeEvent.FullPath)
		if err != nil {
			return err
		}
		nodeEvent.node = node
	case Rename, Remove:
	default:
		return errors.ErrUnsupported
	}

	return nil
}

// lkpChain finds a chain in the event store for a given event
// it uses inode lookup for creates/writes and paths for rename/remove
func lkpChain(nodeEvent *NodeEvent, store EventStore) error {
	var chain *Chain
	var err error

	switch nodeEvent.Type {
	case Create, Write:
		chain, err = store.GetChainByIno(nodeEvent.node.Ino)
	case Remove, Rename:
		chain, err = store.GetChainByPath(nodeEvent.dir.ID, nodeEvent.Path)
	default:
		return errors.ErrUnsupported
	}
	if err != nil {
		return err
	}
	nodeEvent.chain = chain
	return nil
}

// isValidEvent ensures dirEvent is properly initialized for processing
func isValidEvent(nodeEvent *NodeEvent) error {
	node := nodeEvent.node
	chain := nodeEvent.chain
	switch nodeEvent.Type {
	case Create:
		if node == nil {
			return errors.New("create w/ no node")
		}
		if node.Type() < 1 {
			return errors.New(fmt.Sprintf("create unsupported node: %v", node))
		}
	case Write:
		if node == nil {
			return errors.New("write w/ no node")
		}
		if node.Type() < 1 {
			return errors.New(fmt.Sprintf("write unsupported node: %v", node))
		}
		if chain == nil {
			return errors.New(fmt.Sprintf("write w/ no chain"))
		}
	case Rename:
		if chain == nil {
			return errors.New(fmt.Sprintf("rename w/ no chain"))
		}
	case Remove:
		if chain == nil {
			return errors.New(fmt.Sprintf("remove w/ no chain"))
		}
	default:
		return errors.New(fmt.Sprintf("Unsupported event operation: %v", nodeEvent.Type))
	}
	return nil
}

func setEventState(event *Event, node *fnode.Node) {
	switch event.Type {
	case Create:
		// set event node state props
		nodeState := node.State()
		event.ModTime = node.ModTime()
		event.Size = node.Size()
		event.Hash = nodeState.Hash
	case Write:
		// set event node state props
		nodeState := node.State()
		event.ModTime = node.ModTime()
		event.Size = node.Size()
		event.Hash = nodeState.Hash
	default:
	}
}
