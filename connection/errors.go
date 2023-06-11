package connection

import "fmt"

type OnReconnectingError struct {
	Err error
}

type OnReconnectError struct {
	Err error
}

type OnConnectionCloseError struct {
	Err error
}

func (e OnReconnectingError) Error() string {
	return fmt.Sprintf("non library error before reconnecting: %v", e.Err)
}

func (e OnReconnectError) Error() string {
	return fmt.Sprintf("non library error after reconnect: %v", e.Err)
}

func (e OnConnectionCloseError) Error() string {
	return fmt.Sprintf("error on closing previous connection: %v", e.Err)
}
