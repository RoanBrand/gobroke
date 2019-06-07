package store

type memStore struct {
}

func NewMemStore() *memStore{
	return &memStore{}
}

func (s *memStore) Close() error {
	return nil
}

func (s *memStore) AddSubs(client []byte, topics []string, qoss []uint8) error {
	return nil
}