package connection

import (
	"errors"
	"fmt"
)

var (
	ErrOnConnectionReady = errors.New("onConnectionReady is required")
	ErrRetriesExhausted  = errors.New("number of retries to acquire connection exhausted")
)

type OnBeforeConnectError struct {
	Inner error
}

type ConnectInitError struct {
	Inner error
}

type OnConnectionCloseError struct {
	Inner error
}

func (e OnBeforeConnectError) Error() string {
	return fmt.Sprintf("non library error before reconnecting: %v", e.Inner)
}

func (e ConnectInitError) Error() string {
	return fmt.Sprintf("non library error after reconnect: %v", e.Inner)
}

func (e OnConnectionCloseError) Error() string {
	return fmt.Sprintf("error on closing previous connection: %v", e.Inner)
}
