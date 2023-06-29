package connection

import "fmt"

type OnBeforeConnectError struct {
	inner error
}

type ConnectInitError struct {
	inner error
}

type OnConnectionCloseError struct {
	inner error
}

func (e OnBeforeConnectError) Error() string {
	return fmt.Sprintf("non library error before reconnecting: %v", e.inner)
}

func (e ConnectInitError) Error() string {
	return fmt.Sprintf("non library error after reconnect: %v", e.inner)
}

func (e OnConnectionCloseError) Error() string {
	return fmt.Sprintf("error on closing previous connection: %v", e.inner)
}
