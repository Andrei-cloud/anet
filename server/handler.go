package server

// Handler processes incoming messages and returns a response.
type Handler interface {
	HandleMessage(conn *ServerConn, request []byte) ([]byte, error)
}

// HandlerFunc is an adapter to allow the use of ordinary functions as Handlers.
type HandlerFunc func(conn *ServerConn, request []byte) ([]byte, error)

// HandleMessage calls f with the connection and request.
func (f HandlerFunc) HandleMessage(c *ServerConn, request []byte) ([]byte, error) {
	return f(c, request)
}
