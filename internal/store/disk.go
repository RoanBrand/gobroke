package store

import (
	"encoding/binary"
	"github.com/dgraph-io/badger"
	"strconv"
	"time"
)

type diskStore struct {
	db *badger.DB
}

func NewDiskStore(dir string) (*diskStore, error) {
	opts := badger.DefaultOptions
	opts.Dir, opts.ValueDir = dir, dir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &diskStore{db: db}, nil
}

func (s *diskStore) Close() error {
	return s.db.Close()
}

// Load all server subscriptions from store on startup.
func (s *diskStore) LoadSubs(iter func(topic, cID string, subQoS uint8)) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("subs")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			topicL := binary.BigEndian.Uint16(k[4:])
			// cIDL := binary.BigEndian.Uint16(k[6+topicL:])
			val, err := item.Value()
			if err != nil {
				return err
			}

			iter(string(k[6:6+topicL]), string(k[8+topicL:]), val[0])
		}
		return nil
	})
}

// Add subscriptions for client, both to server copy and client sub list.
func (s *diskStore) AddSubs(cID []byte, topics [][]byte, qoss []uint8) error {
	txn := s.db.NewTransaction(true)

	for i, t := range topics {
		// Server subs
		key := make([]byte, 0, 4+len(t)+len(cID))
		key = append(key, []byte("subs")...)
		key = append(key, t...)
		key = append(key, cID...)

		if err := txn.Set(key, qoss[i:i+1]); err != nil {
			if err == badger.ErrTxnTooBig {
				if err = txn.Commit(nil); err != nil {
					txn.Discard()
					return err
				}
				txn = s.db.NewTransaction(true)
				if err = txn.Set(key, qoss[i:i+1]); err != nil {
					txn.Discard()
					return err
				}
			} else {
				txn.Discard()
				return err
			}
		}

		// Client subs
		key = make([]byte, 0, 5+len(cID)+len(t))
		key = append(key, []byte("c")...)
		key = append(key, cID...)
		key = append(key, []byte("subs")...)
		key = append(key, t...)

		if err := txn.Set(key, nil); err != nil {
			if err == badger.ErrTxnTooBig {
				if err = txn.Commit(nil); err != nil {
					txn.Discard()
					return err
				}
				txn = s.db.NewTransaction(true)
				if err = txn.Set(key, nil); err != nil {
					txn.Discard()
					return err
				}
			} else {
				txn.Discard()
				return err
			}
		}
	}

	return txn.Commit(nil)
}

// Remove subscriptions for client, from both server copy and client sub list.
func (s *diskStore) RemoveSubs(cID []byte, topics [][]byte) error {
	txn := s.db.NewTransaction(true)

	for _, t := range topics {
		key := make([]byte, 0, 4+len(t)+len(cID))
		key = append(key, []byte("subs")...)
		key = append(key, t...)
		key = append(key, cID...)

		if err := txn.Delete(key); err != nil {
			if err == badger.ErrTxnTooBig {
				if err = txn.Commit(nil); err != nil {
					txn.Discard()
					return err
				}
				txn = s.db.NewTransaction(true)
				if err = txn.Delete(key); err != nil {
					txn.Discard()
					return err
				}
			} else {
				txn.Discard()
				return err
			}
		}

		key = make([]byte, 0, 5+len(cID)+len(t))
		key = append(key, []byte("c")...)
		key = append(key, cID...)
		key = append(key, []byte("subs")...)
		key = append(key, t...)

		if err := txn.Delete(key); err != nil {
			if err == badger.ErrTxnTooBig {
				if err = txn.Commit(nil); err != nil {
					txn.Discard()
					return err
				}
				txn = s.db.NewTransaction(true)
				if err = txn.Delete(key); err != nil {
					txn.Discard()
					return err
				}
			} else {
				txn.Discard()
				return err
			}
		}
	}

	return txn.Commit(nil)
}

// Get client's current publish ID to use for next message.
/*func (s *diskStore) GetClientPubID(cID []byte) (uint16, error) {
	var pubID uint16
	err := s.db.View(func(txn *badger.Txn) error {
		key := make([]byte, 0, 6+len(cID))
		key = append(key, []byte("c")...)
		key = append(key, cID...)
		key = append(key, []byte("pubID")...)

		item, err := txn.Get(key)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				return err
			}
			pubID = 0
		} else {
			val, err := item.Value()
			if err != nil {
				return err
			}

			pubID = binary.BigEndian.Uint16(val)
		}

		return nil
	})

	return pubID, err
}*/

// Load all stored PUBLISH messages destined for client when client connects to program for first time.
func (s *diskStore) LoadPubs(cID []byte, iter func(p []byte, QoS uint8, pubID uint16)) error {
	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := make([]byte, 0, 4+len(cID))
		prefix = append(prefix, []byte("c")...)
		prefix = append(prefix, cID...)
		prefix = append(prefix, []byte("QoS")...)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			val, err := item.Value()
			if err != nil {
				return err
			}

			qos := k[len(prefix)]
			var pubID uint16
			if qos > 0 {
				pubID = binary.BigEndian.Uint16(k[len(prefix)+1:])
			} else {
				// TODO: make another plan
				err = txn.Delete(k)
				if err != nil {
					return err
				}
			}

			iter(val, qos, pubID)
		}
		return nil
	})
}

// Forward published QoS0,1,2 packet to subscribed client.
// If QoS > 0, provide idx of pubID in packet.
// Returns used pubID if QoS > 0.
func (s *diskStore) ForwardPub(cID, packet []byte, QoS uint8, pubIDIdx uint32) (uint16, error) {
	var pubID uint16
	err := s.db.Update(func(txn *badger.Txn) error {
		if QoS > 0 {
			// get client pubID.
			key := make([]byte, 0, 6+len(cID))
			key = append(key, []byte("c")...)
			key = append(key, cID...)
			key = append(key, []byte("pubID")...)

			item, err := txn.Get(key)
			if err != nil {
				if err != badger.ErrKeyNotFound {
					return err
				}
				pubID = 1
			} else {
				val, err := item.Value()
				if err != nil {
					return err
				}

				pubID = binary.BigEndian.Uint16(val)
			}

			// increment and set client pubID.
			newID := pubID + 1
			if newID == 0 {
				newID = 1
			}
			err = txn.Set(key, []byte{byte(newID >> 8), byte(newID)})
			if err != nil {
				return err
			}

			// set pubID in pub.
			packet[pubIDIdx], packet[pubIDIdx+1] = byte(pubID>>8), byte(pubID)

			// store pub msg destined for client.
			key = make([]byte, 0, 7+len(cID))
			key = append(key, []byte("c")...)
			key = append(key, cID...)
			key = append(key, []byte("QoS")...)
			key = append(key, QoS, byte(pubID>>8), byte(pubID))

			return txn.Set(key, packet)
		}

		// Qos 0
		key := make([]byte, 0, 13+len(cID))
		key = append(key, []byte("c")...)
		key = append(key, cID...)
		key = append(key, []byte("QoS")...)
		key = append(key, 0)
		key = strconv.AppendInt(key, time.Now().UnixNano(), 10)

		return txn.Set(key, packet)
	})

	return pubID, err
}

func (s *diskStore) RemovePub(cID []byte, QoS uint8, pubID uint16) error {
	txn := s.db.NewTransaction(true)
	key := make([]byte, 0, 7+len(cID))
	key = append(key, []byte("c")...)
	key = append(key, cID...)
	key = append(key, []byte("QoS")...)
	key = append(key, QoS, byte(pubID>>8), byte(pubID))

	if err := txn.Delete(key); err != nil {
		return err
	}

	return txn.Commit(nil)
}
