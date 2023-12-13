package gobroke

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	log "github.com/sirupsen/logrus"

	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/RoanBrand/gobroke/internal/queue"
)

func (s *Server) diskInit() error {
	hd, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	db, err := pebble.Open(filepath.Join(hd, "gobrokeTEST"), &pebble.Options{})
	if err != nil {
		return err
	}

	s.db = db
	return nil
}

func (s *Server) diskNewClient(c *client) error {
	s.dbClientIdCnt++
	k := []byte{'s', 'c', 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(k[2:], s.dbClientIdCnt)
	c.dbClientId = s.dbClientIdCnt

	pb := s.db.NewBatch()
	pb.Set(k, []byte(c.session.clientId), pebble.Sync)
	pb.Set([]byte{'c', 'i'}, k[2:], pebble.Sync)

	return pb.Commit(pebble.Sync)
}

func (s *Server) diskLoadClients() error {
	if cidB, closer, err := s.db.Get([]byte{'c', 'i'}); err != nil {
		if err != pebble.ErrNotFound {
			return err
		}
	} else {
		defer closer.Close()
		s.dbClientIdCnt = binary.BigEndian.Uint64(cidB)
	}

	dbIt, err := s.db.NewIter(prefixIterOptions([]byte{'s', 'c'}))
	if err != nil {
		return err
	}

	s.dbClients = make(map[uint64]*client)

	for dbIt.First(); dbIt.Valid(); dbIt.Next() {
		cid := binary.BigEndian.Uint64(dbIt.Key()[2:])
		clientId := string(dbIt.Value())

		c := s.newClient(&session{clientId: clientId})
		c.dbClientId = cid
		s.clients[clientId] = c
		s.dbClients[cid] = c

		log.Println("LOADING client", clientId, c.dbClientId, c.dbMsgRxIdCnt)
	}

	return dbIt.Close()
}

func (s *Server) diskDeleteClient(c *client) error {
	delete(s.dbClients, c.dbClientId)
	k := []byte{'s', 'c', 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(k[2:], c.dbClientId)

	return s.db.Delete(k, pebble.Sync)
}

func (s *Server) diskNewSub(t []byte, sub *subscription, cId uint64) error {
	k := []byte{'s', 's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s.dbSubIdCnt++
	binary.BigEndian.PutUint64(k[2:], s.dbSubIdCnt)
	sub.dbSubId = s.dbSubIdCnt

	binary.BigEndian.PutUint64(k[10:], cId)
	v := make([]byte, 1, 1+len(t))
	v[0] = sub.options
	v = append(v, t...)

	pb := s.db.NewBatch()
	pb.Set(k, v, pebble.Sync)
	pb.Set([]byte{'s', 'i'}, k[2:10], pebble.Sync)

	return pb.Commit(pebble.Sync)
}

func (s *Server) diskLoadSubs() error {
	if sidB, closer, err := s.db.Get([]byte{'s', 'i'}); err != nil {
		if err != pebble.ErrNotFound {
			return err
		}
	} else {
		defer closer.Close()
		s.dbSubIdCnt = binary.BigEndian.Uint64(sidB)
	}

	dbIt, err := s.db.NewIter(prefixIterOptions([]byte{'s', 's'}))
	if err != nil {
		return err
	}

	for dbIt.First(); dbIt.Valid(); dbIt.Next() {
		sId := binary.BigEndian.Uint64(dbIt.Key()[2:])
		cId := binary.BigEndian.Uint64(dbIt.Key()[10:])
		op := dbIt.Value()[0]
		topicF := dbIt.Value()[1:]

		c, ok := s.dbClients[cId]
		if !ok {
			log.Println("SKIPPING subscription", string(topicF), "for unknown cId", cId)
			s.diskDeleteSub(sId, cId)
			continue
		}

		err = s.addSubscriptions(c, [][]byte{topicF}, []uint8{op}, 0, true)
		if err != nil {
			return err
		}

		log.Println("LOADING subscription", string(topicF), "for", c.session.clientId)
	}

	return dbIt.Close()
}

func (s *Server) diskDeleteSub(sId, cId uint64) error {
	k := []byte{'s', 's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(k[2:], sId)
	binary.BigEndian.PutUint64(k[10:], cId)

	return s.db.Delete(k, pebble.Sync)
}

// need to commit later after clients got processed.
func (s *Server) diskNewPublishedMsg(p *model.PubMessage) error {
	s.dbPubIdCnt++
	k := []byte{'s', 'p', 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(k[2:], s.dbPubIdCnt)
	p.DbSPubId = s.dbPubIdCnt

	v := make([]byte, 16, 16+len(p.B)) // TODO: props
	binary.BigEndian.PutUint64(v, uint64(p.Expiry))
	binary.BigEndian.PutUint64(v[8:], p.DbPubberId)
	v = append(v, p.B...)

	s.dbBatch = s.db.NewBatch()
	s.dbBatch.Set(k, v, pebble.Sync)
	return s.dbBatch.Set([]byte{'p', 'i'}, k[2:], pebble.Sync)
}

func (s *Server) diskNewPublishedMsgToClient(c *client, i *queue.Item, retained bool) error {
	k := []byte{'c', 'p', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	c.dbMsgRxIdCnt++
	binary.BigEndian.PutUint64(k[2:], c.dbMsgRxIdCnt)
	i.DbMsgTxId = c.dbMsgRxIdCnt

	binary.BigEndian.PutUint64(k[10:], c.dbClientId)
	v := make([]byte, 8+1+2)
	binary.BigEndian.PutUint64(v, i.P.DbSPubId)
	v[8] = i.TxQoS
	if i.Retained {
		v[8] |= 0x08
	}

	if retained { // not new msg that is part of batch
		return s.db.Set(k, v, pebble.Sync)
	}

	s.dbBatch.Set(k, v, pebble.Sync)

	k2 := []byte{'m', 'i', 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(k2[2:], c.dbClientId)
	v2 := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(v2, c.dbMsgRxIdCnt)

	return s.dbBatch.Set(k2, v2, pebble.Sync)
}

func (s *Server) diskCommitNewPubMsg() error {
	return s.dbBatch.Commit(pebble.Sync)
}

func (s *Server) diskLoadPubbedMsgs() error {
	// Server RXed msgs Id
	if pidB, closer, err := s.db.Get([]byte{'p', 'i'}); err != nil {
		if err != pebble.ErrNotFound {
			return err
		}
	} else {
		defer closer.Close()
		s.dbPubIdCnt = binary.BigEndian.Uint64(pidB)

		log.Println("LOADING dbPubIdCnt", s.dbPubIdCnt)
	}

	// Client msgs to send to client Id
	dbIt, err := s.db.NewIter(prefixIterOptions([]byte{'m', 'i'}))
	if err != nil {
		return err
	}

	for dbIt.First(); dbIt.Valid(); dbIt.Next() {
		cId := binary.BigEndian.Uint64(dbIt.Key()[2:])
		mid := binary.BigEndian.Uint64(dbIt.Value())

		c, ok := s.dbClients[cId]
		if !ok {
			log.Println("SKIPPING client dbMsgRxIdCnt for unknown cId", cId)
			s.db.Delete(dbIt.Key(), pebble.NoSync)
			continue
		}

		c.dbMsgRxIdCnt = mid

		log.Println("LOADING client dbMsgRxIdCnt", mid, "for cId", cId)
	}

	if err = dbIt.Close(); err != nil {
		return err
	}

	// Client msgs to send out to client
	dbIt, err = s.db.NewIter(prefixIterOptions([]byte{'c', 'p'}))
	if err != nil {
		return err
	}

	pMsgs := make(map[uint64]*model.PubMessage)

	for dbIt.First(); dbIt.Valid(); dbIt.Next() {
		cmId := binary.BigEndian.Uint64(dbIt.Key()[2:])
		cId := binary.BigEndian.Uint64(dbIt.Key()[10:])
		pId := binary.BigEndian.Uint64(dbIt.Value())
		finalQoS := dbIt.Value()[8] & 0x03
		retained := dbIt.Value()[8]&0x08 > 0

		c, ok := s.dbClients[cId]
		if !ok {
			log.Println("SKIPPING pubbed msg", cmId, pId, "for unknown cId", cId)
			s.db.Delete(dbIt.Key(), pebble.Sync)
			continue
		}

		pMsg, ok := pMsgs[pId]
		if !ok {
			kP := []byte{'s', 'p', 0, 0, 0, 0, 0, 0, 0, 0}
			binary.BigEndian.PutUint64(kP[2:], pId)
			pMsgB, closer, err := s.db.Get(kP)
			if err != nil {
				if err == pebble.ErrNotFound {
					log.Println("SKIPPING pubbed msg", cmId, "for unknown pmId", pId)
					s.db.Delete(dbIt.Key(), pebble.Sync)
					continue
				}
				return err
			}

			pMsg = &model.PubMessage{
				Expiry:     int64(binary.BigEndian.Uint64(pMsgB)),
				B:          make([]byte, len(pMsgB)-16),
				DbPubberId: binary.BigEndian.Uint64(pMsgB[8:]),
				DbSPubId:   pId,
			}
			copy(pMsg.B, pMsgB[16:])
			pMsg.Publisher = s.dbClients[pMsg.DbPubberId].session.clientId

			pMsgs[pId] = pMsg

			closer.Close()
		}

		//s.processPub(c, pMsg, subscription{}, retained)
		i := queue.GetItem(pMsg)
		i.TxQoS, i.Retained = finalQoS, retained
		pMsg.AddUser()
		switch finalQoS {
		case 0:
			c.q0.Add(i)
		case 1:
			c.q1.Add(i)
		case 2:
			c.q2.Add(i)
		}

		log.Println("LOADING pubbed msg", cmId, pId, "for", c.session.clientId)
	}

	return dbIt.Close()

}

func (s *Server) diskDeleteServerMsg(dbPubId uint64) error {
	k := []byte{'s', 'p', 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(k[2:], dbPubId)

	return s.db.Delete(k, pebble.Sync)
}

func (s *Server) diskDeleteClientMsg(dbCId, dbMsgId uint64) error {
	k := []byte{'c', 'p', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(k[2:], dbMsgId)
	binary.BigEndian.PutUint64(k[10:], dbCId)

	return s.db.Delete(k, pebble.Sync)
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}

func prefixIterOptions(prefix []byte) *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: keyUpperBound(prefix),
	}
}
