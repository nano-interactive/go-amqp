package consumer

import (
	"errors"
	"fmt"
)

var (
	ErrQueueNameRequired         = errors.New("queue name is required... Please call WithQueueName(queueName) option function")
	ErrOnMessageCallbackRequired = errors.New("onMessageError is required")
	ErrMessageTypeInvalid        = errors.New("message type must be a value type")
)

type (
	QueueDeclarationError    struct{ Inner error }
	ListenerStartFailedError struct{ Inner error }
)

func (e *QueueDeclarationError) Error() string {
	return fmt.Sprintf("queue declaration error: %v", e.Inner)
}

func (e *ListenerStartFailedError) Error() string {
	return fmt.Sprintf("failed to start listener: %v", e.Inner)
}
