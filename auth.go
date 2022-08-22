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
	//AuthSubscription(clientId string, topicFilter []byte) error
}
