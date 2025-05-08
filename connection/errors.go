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

func (e *ReconnectError) Error() string {
	return fmt.Sprintf("reconnection failed: %v", e.Inner)
}

func (e *ReconnectError) Unwrap() error {
	return e.Inner
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("connection blocked: reason=%s, active=%v", e.Blocked.Reason, e.Blocked.Active)
}

func (e *OnBeforeConnectError) Unwrap() error {
	return e.Inner
}

func (e *ConnectInitError) Unwrap() error {
	return e.Inner
}
