package publisher_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/nano-interactive/go-amqp/v3/connection"
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
		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange", "test_queue")

		pub, err := publisher.New[Msg](context.TODO(), connection.DefaultConfig, mappings.Exchange("test_exchange"))

		assert.NoError(err)
		assert.NotNil(pub)
	})

	t.Run("WithOptions", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t).
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
				Host:           "localhost",
				Port:           1234,
				ReconnectRetry: 2,
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

		mappings := amqp_testing.NewMappings(t).
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

		mappings := amqp_testing.NewMappings(t).
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

		mappings := amqp_testing.NewMappings(t).
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
}

func TestPublisherClose(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t).
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
		mappings := amqp_testing.NewMappings(t).
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

	t.Run("Multiple_Close_Calling", func(t *testing.T) {
		t.Parallel()

		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange_multiple_close_call", "test_queue_multiple_close_call")

		pub, err := publisher.New[Msg](
			context.TODO(),
			connection.DefaultConfig,
			mappings.Exchange("test_exchange_multiple_close_call"),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		assert.NoError(pub.Close())
		assert.NoError(pub.Close())
		assert.NoError(pub.Close())
	})
}
