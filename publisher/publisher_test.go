package publisher_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/consumer"
	"github.com/nano-interactive/go-amqp/v3/publisher"
	"github.com/nano-interactive/go-amqp/v3/serializer"
	amqp_testing "github.com/nano-interactive/go-amqp/v3/testing"
)

type Msg struct {
	Name string `json:"name"`
}

type MockSerializer struct {
	mock.Mock
}

func (m *MockSerializer) Marshal(msg Msg) ([]byte, error) {
	args := m.Called(msg)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockSerializer) Unmarshal(data []byte) (Msg, error) {
	args := m.Called(data)
	return args.Get(0).(Msg), args.Error(1)
}

func (m *MockSerializer) GetContentType() string {
	return m.Called().String(0)
}

func TestPublisherNew(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()
		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMapping("test_exchange", "test_queue")

		pub, err := publisher.New[Msg](context.TODO(), connection.DefaultConfig, mappings.Exchange("test_exchange"))

		assert.NoError(err)
		assert.NotNil(pub)
	})

	t.Run("WithOptions", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMapping("test_exchange", "test_queue")

		pub, err := publisher.New[Msg](
			context.TODO(),
			connection.DefaultConfig,
			mappings.Exchange("test_exchange"),
			publisher.WithSerializer[Msg](serializer.JSON[Msg]{}),
		)

		assert.NoError(err)
		assert.NotNil(pub)
	})

	t.Run("ConnectionFailed", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		t.Cleanup(cancel)

		pub, err := publisher.New[Msg](
			ctx,
			connection.Config{
				Host:              "localhost",
				Port:              1234,
				ReconnectRetry:    2,
				ReconnectInterval: 100 * time.Millisecond,
			},
			"test_exchange",
		)

		assert.Error(err)
		assert.Nil(pub)
	})
}

func TestPublisherPublish(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMapping("test_exchange_basic", "test_queue_basic")

		pub, err := publisher.New[Msg](context.TODO(), connection.DefaultConfig, mappings.Exchange("test_exchange_basic"))
		assert.NoError(err)
		assert.NotNil(pub)

		assert.NoError(pub.Publish(context.Background(), Msg{Name: "test"}))
		assert.NoError(pub.Close())

		messages := amqp_testing.ConsumeAMQPMessages[Msg](t, mappings.Queue("test_queue_basic"), connection.DefaultConfig, 200*time.Millisecond)

		assert.Len(messages, 1)
		assert.Equal("test", messages[0].Name)
	})

	t.Run("WithSerializer", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMapping("test_exchange_serializer", "test_queue_serializer")

		mockSerializer := &MockSerializer{}

		pub, err := publisher.New[Msg](
			context.TODO(),
			connection.DefaultConfig,
			mappings.Exchange("test_exchange_serializer"),
			publisher.WithSerializer[Msg](mockSerializer),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		mockSerializer.On("Marshal", Msg{Name: "test"}).
			Once().
			Return([]byte("\"test\""), nil)

		mockSerializer.On("GetContentType").
			Once().
			Return("application/json")

		assert.NoError(pub.Publish(context.Background(), Msg{Name: "test"}))
		assert.NoError(pub.Close())

		messages := amqp_testing.ConsumeAMQPMessages[string](
			t,
			mappings.Queue("test_queue_serializer"),
			connection.DefaultConfig,
			200*time.Millisecond,
		)

		assert.Len(messages, 1)
		assert.Equal("test", messages[0])
		mockSerializer.AssertExpectations(t)
	})

	t.Run("WithSerializerFails", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMapping("test_exchange_serializer_fails", "test_queue_serializer_fails")

		mockSerializer := &MockSerializer{}

		pub, err := publisher.New[Msg](
			context.TODO(),
			connection.DefaultConfig,
			mappings.Exchange("test_exchange_serializer_fails"),
			publisher.WithSerializer[Msg](mockSerializer),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		//nolint:goerr113
		expectedErr := errors.New("failed to serialize")

		mockSerializer.On("Marshal", Msg{Name: "test"}).
			Once().
			Return([]byte{}, expectedErr)

		assert.Error(pub.Publish(context.Background(), Msg{Name: "test"}), expectedErr)
		assert.NoError(pub.Close())

		messages := amqp_testing.ConsumeAMQPMessages[string](
			t,
			mappings.Queue("test_queue_serializer_fails"),
			connection.DefaultConfig,
			200*time.Millisecond,
		)

		assert.Len(messages, 0)
		mockSerializer.AssertNotCalled(t, "GetContentType")
		mockSerializer.AssertExpectations(t)
	})

	t.Run("WithRoutingKey_Default", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMappings(
				amqp_testing.PublisherConfig{
					Name: "test_exchange_rk_default",
					Config: publisher.ExchangeDeclare{
						RoutingKey: "rk.default",
						Type:       publisher.ExchangeTypeDirect,
						Durable:    true,
					},
				},
				[]amqp_testing.ConsumerConfig{
					{
						Name: "test_queue_rk_default",
						Config: consumer.QueueDeclare{
							QueueName: "test_queue_rk_default",
							Durable:   true,
						},
					},
				},
			)

		pub, err := publisher.New[Msg](
			context.TODO(),
			connection.DefaultConfig,
			mappings.Exchange("test_exchange_rk_default"),
			publisher.WithExchangeDeclare[Msg](publisher.ExchangeDeclare{
				RoutingKey: "rk.default",
				Type:       publisher.ExchangeTypeDirect,
				Durable:    true,
			}),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		assert.NoError(pub.Publish(context.Background(), Msg{Name: "routed"}))
		assert.NoError(pub.Close())

		messages := amqp_testing.ConsumeAMQPMessages[Msg](t, mappings.Queue("test_queue_rk_default"), connection.DefaultConfig, 200*time.Millisecond)
		assert.Len(messages, 1)
		assert.Equal("routed", messages[0].Name)
	})

	t.Run("WithRoutingKey_Override", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMappings(
				amqp_testing.PublisherConfig{
					Name: "test_exchange_rk_override",
					Config: publisher.ExchangeDeclare{
						RoutingKey: "rk.a",
						Type:       publisher.ExchangeTypeDirect,
						Durable:    true,
					},
				},
				[]amqp_testing.ConsumerConfig{
					{
						Name: "test_queue_rk_a",
						Config: consumer.QueueDeclare{
							QueueName: "test_queue_rk_a",
							Durable:   true,
						},
					},
				},
			).
			AddMappings(
				amqp_testing.PublisherConfig{
					Name: "test_exchange_rk_override",
					Config: publisher.ExchangeDeclare{
						RoutingKey: "rk.b",
						Type:       publisher.ExchangeTypeDirect,
						Durable:    true,
					},
				},
				[]amqp_testing.ConsumerConfig{
					{
						Name: "test_queue_rk_b",
						Config: consumer.QueueDeclare{
							QueueName: "test_queue_rk_b",
							Durable:   true,
						},
					},
				},
			)

		pub, err := publisher.New[Msg](
			context.TODO(),
			connection.DefaultConfig,
			mappings.Exchange("test_exchange_rk_override"),
			publisher.WithExchangeDeclare[Msg](publisher.ExchangeDeclare{
				RoutingKey: "rk.a",
				Type:       publisher.ExchangeTypeDirect,
				Durable:    true,
			}),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		// Publish to rk.a (default)
		assert.NoError(pub.Publish(context.Background(), Msg{Name: "msg-a"}))
		// Publish to rk.b (per-call override)
		assert.NoError(pub.Publish(context.Background(), Msg{Name: "msg-b"}, publisher.WithRoutingKey("rk.b")))
		assert.NoError(pub.Close())

		messagesA := amqp_testing.ConsumeAMQPMessages[Msg](t, mappings.Queue("test_queue_rk_a"), connection.DefaultConfig, 200*time.Millisecond)
		messagesB := amqp_testing.ConsumeAMQPMessages[Msg](t, mappings.Queue("test_queue_rk_b"), connection.DefaultConfig, 200*time.Millisecond)

		assert.Len(messagesA, 1)
		assert.Equal("msg-a", messagesA[0].Name)

		assert.Len(messagesB, 1)
		assert.Equal("msg-b", messagesB[0].Name)
	})
}

func TestPublisherClose(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMapping("test_exchange_close", "test_queue_close")

		pub, err := publisher.New[Msg](
			context.TODO(),
			connection.DefaultConfig,
			mappings.Exchange("test_exchange_close"),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		assert.NoError(pub.Close())
	})

	t.Run("Call_To_Publish_After_Close", func(t *testing.T) {
		t.Parallel()
		mappings := amqp_testing.NewMappings(t, connection.DefaultConfig).
			AddMapping("test_exchange_after_close", "test_queue_after_close")

		pub, err := publisher.New[Msg](
			context.TODO(),
			connection.DefaultConfig,
			mappings.Exchange("test_exchange_after_close"),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		assert.NoError(pub.Close())
		assert.ErrorIs(pub.Publish(context.Background(), Msg{Name: "test"}), publisher.ErrClosed)
	})
}
