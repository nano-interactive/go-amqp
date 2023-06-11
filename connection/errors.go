package connection

import "fmt"

type OnBeforeConnectError struct {
	Err error
}

type ConnectInitError struct {
	Err error
}

type OnConnectionCloseError struct {
	Err error
}

func (e OnBeforeConnectError) Error() string {
	return fmt.Sprintf("non library error before reconnecting: %v", e.Err)
}

func (e ConnectInitError) Error() string {
	return fmt.Sprintf("non library error after reconnect: %v", e.Err)
}

func (e OnConnectionCloseError) Error() string {
	return fmt.Sprintf("error on closing previous connection: %v", e.Err)
}
