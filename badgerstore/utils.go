package badgerstore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	badger "github.com/dgraph-io/badger/v4"
)

func makeKey(comps ...[]byte) []byte {
	key := comps[0]
	for _, comp := range comps[1:] {
		key = append(append(key, []byte(":")...), comp...)
	}
	return key
}

func getNextIDFor(txn *badger.Txn, prefix []byte) ([]byte, error) {
	item, err := txn.Get(append([]byte("counter:"), prefix...))
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return uint64ToBytes(0), err
		}
		return uint64ToBytes(1), nil
	}
	nextID := uint64(0)
	err = item.Value(func(val []byte) error {
		nextID = binary.BigEndian.Uint64(val) + 1
		return nil
	})
	return uint64ToBytes(nextID), err
}

func setIDCounterFor(txn *badger.Txn, idBytes []byte, prefix []byte) error {
	return txn.Set(append([]byte("counter:"), prefix...), idBytes)
}

func iterPrefix[T any](txn *badger.Txn, prefix []byte) ([][]byte, []T, error) {
	ids := make([][]byte, 0)
	objs := make([]T, 0)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()
		id := key[len(prefix)+1:]
		obj, err := parseItem[T](item)
		if err != nil {
			return nil, nil, err
		}
		ids = append(ids, id)
		objs = append(objs, *obj)
	}
	return ids, objs, nil
}

// get object from store by key
func getObject[T any](txn *badger.Txn, key []byte) (*T, error) {
	value, err := getValue(txn, key)
	if value == nil || err != nil {
		return nil, err
	}
	return parseValue[T](value)
}

// get object from value
// if the object doesn't exist returns nil, nil
// if another error occurs returns nil, error
func parseValue[T any](value []byte) (*T, error) {
	object, err := decode[T](value)
	if err != nil {
		return nil, err
	}
	return &object, nil
}

// get object from item
func parseItem[T any](item *badger.Item) (*T, error) {
	value, err := copyValue(item)
	if err != nil {
		return nil, err
	}
	return parseValue[T](value)
}

// get a value from the store by key
func getValue(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := getItem(txn, key)
	if item == nil || err != nil {
		return nil, err
	}
	return copyValue(item)
}

// simple wrapper around ValueCopy
func copyValue(item *badger.Item) ([]byte, error) {
	return item.ValueCopy(nil)
}

// simple wrapper around txn.
// returns (nil, nil) if value doesn't exist (on ErrKeyNotFound)
// returns (nil, err) if another error occurs
func getItem(txn *badger.Txn, key []byte) (*badger.Item, error) {
	item, err := txn.Get(key)
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return nil, err
		}
		return nil, nil
	}
	return item, nil
}

func addObject[T any](txn *badger.Txn, key []byte, obj T) error {
	value, err := encode(obj)
	if err != nil {
		return err
	}
	if err = txn.Set(key, value); err != nil {
		return err
	}
	return nil
}

func encode[T any](from T) ([]byte, error) {
	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	err := e.Encode(from)
	return b.Bytes(), err
}

func decode[T any](from []byte) (T, error) {
	var decoded T
	d := gob.NewDecoder(bytes.NewReader(from))
	err := d.Decode(&decoded)
	return decoded, err
}

func uint64ToBytes(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return buf
}
