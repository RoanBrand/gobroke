package gobroke

// Provider of Authentication and Authorization for the Server.
// Callers must return non-nil errors if Auth fails.
type Auther interface {
	// Authenticate a user (client) trying to connect to the server.
	// username and password is optional and can be nil.
	AuthUser(clientId string, username, password []byte) error

	// Authorizes a client for publishing to a Topic Name.
	AuthPublish(clientId string, topicName []byte) error

	// Authorizes a client for subscribing to a Topic Filter.
	AuthSubscription(clientId string, topicFilter []byte) error
}

// Example implementation:
/*
	type auther struct{}

	func (a *auther) AuthUser(clientId string, username, password []byte) error {
		if string(username) == "roan" && string(password) == "brand" {
			return nil
		}
		return errors.New("not whitelisted")
	}

	func (a *auther) AuthPublish(clientId string, topicName []byte) error {
		return nil
	}

	func (a *auther) AuthSubscription(clientId string, topicFilter []byte) error {
		if clientId != "goodClient" && string(topicFilter) == "restricted" {
			return errors.New("restricted")
		}
		return nil
	}

	func main() {
		s := gobroke.Server{Auther: new(auther)}
		err := s.Run()
		if err != nil {
			panic(err)
		}
	}
*/
