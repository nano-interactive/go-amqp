package publisher_test

import (
	"testing"

	"github.com/nano-interactive/go-amqp/v3/publisher"
	"github.com/stretchr/testify/require"
)

func TestExchangeTypeStrin(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	assert.Equal(publisher.ExchangeTypeDirect.String(), "direct")
	assert.Equal(publisher.ExchangeTypeFanout.String(), "fanout")
	assert.Equal(publisher.ExchangeTypeTopic.String(), "topic")
	assert.Equal(publisher.ExchangeTypeHeader.String(), "headers")
	assert.Panics(func() {
		_ = publisher.ExchangeType(4).String()
	})
}
