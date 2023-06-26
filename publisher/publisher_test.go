package publisher_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/nano-interactive/go-amqp/v2/publisher"
	"github.com/nano-interactive/go-amqp/v2/serializer"
	amqp_testing "github.com/nano-interactive/go-amqp/v2/testing"
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
		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange", "test_queue")

		pub, err := publisher.New[Msg](mappings.Exchange("test_exchange"))

		assert.NoError(err)
		assert.NotNil(pub)
	})

	t.Run("WithOptions", func(t *testing.T) {
		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange", "test_queue")

		pub, err := publisher.New[Msg](
			mappings.Exchange("test_exchange"),
			publisher.WithContext[Msg](context.Background()),
			publisher.WithSerializer[Msg](serializer.JsonSerializer[Msg]{}),
		)

		assert.NoError(err)
		assert.NotNil(pub)
	})
}

func TestPublisherPublish(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	t.Run("Basic", func(t *testing.T) {
		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange", "test_queue")

		pub, err := publisher.New[Msg](mappings.Exchange("test_exchange"))
		assert.NoError(err)
		assert.NotNil(pub)

		assert.NoError(pub.Publish(context.Background(), Msg{Name: "test"}))
		assert.NoError(pub.Close())

		messages := amqp_testing.ConsumeAMQPMessages[Msg](t, mappings.Queue("test_queue"), 200*time.Millisecond)

		assert.Len(messages, 1)
		assert.Equal("test", messages[0].Name)
	})

	t.Run("WithSerializer", func(t *testing.T) {
		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange", "test_queue")

		serializer := &MockSerializer{}

		pub, err := publisher.New[Msg](
			mappings.Exchange("test_exchange"),
			publisher.WithSerializer[Msg](serializer),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		serializer.On("Marshal", Msg{Name: "test"}).
			Once().
			Return([]byte("\"test\""), nil)

		serializer.On("GetContentType").
			Once().
			Return("application/json")

		assert.NoError(pub.Publish(context.Background(), Msg{Name: "test"}))
		assert.NoError(pub.Close())

		messages := amqp_testing.ConsumeAMQPMessages[string](
			t,
			mappings.Queue("test_queue"),
			200*time.Millisecond,
		)

		assert.Len(messages, 1)
		assert.Equal("test", messages[0])
		serializer.AssertExpectations(t)
	})

	t.Run("WithSerializerFails", func(t *testing.T) {
		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange", "test_queue")

		serializer := &MockSerializer{}

		pub, err := publisher.New[Msg](
			mappings.Exchange("test_exchange"),
			publisher.WithSerializer[Msg](serializer),
		)
		assert.NoError(err)
		assert.NotNil(pub)

		expectedErr := errors.New("failed to serialize")

		serializer.On("Marshal", Msg{Name: "test"}).
			Once().
			Return([]byte{}, expectedErr)

		assert.Error(pub.Publish(context.Background(), Msg{Name: "test"}), expectedErr)
		assert.NoError(pub.Close())

		messages := amqp_testing.ConsumeAMQPMessages[string](
			t,
			mappings.Queue("test_queue"),
			200*time.Millisecond,
		)

		assert.Len(messages, 0)
		serializer.AssertNotCalled(t, "GetContentType")
		serializer.AssertExpectations(t)
	})
}

func TestPublisherClose(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	t.Run("Basic", func(t *testing.T) {
		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange", "test_queue")

		pub, err := publisher.New[Msg](mappings.Exchange("test_exchange"))
		assert.NoError(err)
		assert.NotNil(pub)

		assert.NoError(pub.Close())
	})

	t.Run("Multiple_Close_Calling", func(t *testing.T) {
		mappings := amqp_testing.NewMappings(t).
			AddMapping("test_exchange", "test_queue")

		pub, err := publisher.New[Msg](mappings.Exchange("test_exchange"))
		assert.NoError(err)
		assert.NotNil(pub)

		assert.NoError(pub.Close())
		assert.NoError(pub.Close())
		assert.NoError(pub.Close())
	})
}
