package auth

import (
	"errors"
	"sync"
)

type basicAuth struct {
	users map[string]*credential // k: clientId
	userL sync.RWMutex

	subs map[string]map[string]struct{} // topicF -> clientIds
	subL sync.RWMutex

	pubs map[string]map[string]struct{}
	pubL sync.RWMutex

	allowGuests bool
}

type credential struct {
	userName string
	password string
}

func NewBasicAuth() *basicAuth {
	return &basicAuth{
		users: make(map[string]*credential),
		subs:  make(map[string]map[string]struct{}),
		pubs:  make(map[string]map[string]struct{}),
	}
}

func (ba *basicAuth) RegisterUser(clientId, userName, password string) {
	ba.userL.Lock()
	ba.users[clientId] = &credential{userName, password}
	ba.userL.Unlock()
}

func (ba *basicAuth) RemoveUser(clientId string) {
	ba.userL.Lock()
	delete(ba.users, clientId)
	ba.userL.Unlock()
}

// Allow user to subscribe to a restricted Topic Filter.
// The Topic Filter becomes restricted if it is called with this function.
func (ba *basicAuth) AllowSubscription(topicFilter, clientId string) {
	ba.subL.Lock()

	clients, ok := ba.subs[topicFilter]
	if !ok {
		clients = make(map[string]struct{})
		ba.subs[topicFilter] = clients
	}

	clients[clientId] = struct{}{}
	ba.subL.Unlock()
}

// Allow user to publish to a restricted Topic Name.
// The Topic Name becomes restricted if it is called with this function.
func (ba *basicAuth) AllowPublish(topicName, clientId string) {
	ba.pubL.Lock()

	clients, ok := ba.pubs[topicName]
	if !ok {
		clients = make(map[string]struct{})
		ba.pubs[topicName] = clients
	}

	clients[clientId] = struct{}{}
	ba.pubL.Unlock()
}

// Allow unregistered user access. (Users without username/password)
// Will not allow guests to join with clientIds that are registered, regardless.
func (ba *basicAuth) ToggleGuestAccess(allow bool) {
	ba.allowGuests = allow
}

// Auth

func (ba *basicAuth) AuthUser(clientId string, username, password []byte) error {
	ba.userL.RLock()
	user, ok := ba.users[clientId]
	ba.userL.RUnlock()

	if ok {
		if string(username) != user.userName || string(password) != user.password {
			return errors.New("bad username and/or password")
		}
	} else if !ba.allowGuests {
		return errors.New("unknown user")
	}

	return nil
}

func (ba *basicAuth) AuthPublish(clientId string, topicName []byte) error {
	ba.pubL.RLock()
	clients, ok := ba.pubs[string(topicName)]
	if !ok {
		ba.pubL.RUnlock()
		return nil
	}

	_, ok = clients[clientId]
	ba.pubL.RUnlock()

	if !ok {
		return errors.New("restricted")
	}

	return nil
}

func (ba *basicAuth) AuthSubscription(clientId string, topicFilter []byte) error {
	ba.subL.RLock()
	clients, ok := ba.subs[string(topicFilter)]
	if !ok {
		ba.subL.RUnlock()
		return nil
	}

	_, ok = clients[clientId]
	ba.subL.RUnlock()

	if !ok {
		return errors.New("restricted")
	}

	return nil
}
