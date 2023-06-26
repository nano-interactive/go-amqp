package publisher_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/nano-interactive/go-amqp/v2/publisher"
	amqp_testing "github.com/nano-interactive/go-amqp/v2/testing"
)

type Msg struct {
	Name string `json:"name"`
}

func TestPublisherNew_Basic(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	mappings := amqp_testing.NewMappings(t).
		AddMapping("test_exchange", "test_queue")

	pub, err := publisher.New[Msg](mappings.Exchange("test_exchange"))

	assert.NoError(err)
	assert.NotNil(pub)
}
