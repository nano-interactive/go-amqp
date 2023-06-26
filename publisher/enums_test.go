package publisher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExchangeTypeStrin(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	assert.Equal(ExchangeTypeDirect.String(), "direct")
	assert.Equal(ExchangeTypeFanout.String(), "fanout")
	assert.Equal(ExchangeTypeTopic.String(), "topic")
	assert.Panics(func() {
		_ = ExchangeType(4).String()
	})
}