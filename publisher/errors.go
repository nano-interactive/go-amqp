package publisher

import "errors"

var (
	ErrNoRetry              = errors.New("no retry")
	ErrExchangeNameRequired = errors.New("exchange name is required")
)
