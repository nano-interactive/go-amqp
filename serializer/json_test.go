package serializer_test

import (
	"testing"

	"github.com/nano-interactive/go-amqp/v3/serializer"
	"github.com/stretchr/testify/require"
)

type message struct {
	Name string `json:"name"`
}

var ser serializer.Serializer[message] = serializer.JSON[message]{}

func TestSerializerSuccess(t *testing.T) {
	t.Parallel()

	// Arrange
	assert := require.New(t)
	data := message{Name: "Test"}

	// Act
	json, err := ser.Marshal(data)

	// Assert
	assert.NoError(err)
	assert.NotNil(json)
	assert.JSONEq(string(json), `{"name": "Test"}`)
}

func TestSerializerEmpty(t *testing.T) {
	t.Parallel()

	// Arrange
	assert := require.New(t)
	data := message{Name: ""}

	// Act
	json, err := ser.Marshal(data)

	// Assert
	assert.NoError(err)
	assert.NotNil(json)
	assert.JSONEq(string(json), `{"name": ""}`)
}

func TestDeSerializerSuccess(t *testing.T) {
	t.Parallel()

	// Arrange
	assert := require.New(t)

	// Act
	data, err := ser.Unmarshal([]byte(`{"name": "Test"}`))

	// Assert
	assert.NoError(err)
	assert.NotNil(data)
}

func TestDeSerializerFail(t *testing.T) {
	t.Parallel()

	// Arrange
	assert := require.New(t)

	// Act
	data, err := ser.Unmarshal([]byte(`{"name": false}`))

	// Assert
	assert.Error(err)
	assert.Empty(data)
}

func TestSerializerContentType(t *testing.T) {
	t.Parallel()

	// Arrange
	assert := require.New(t)

	// Act
	contentType := ser.GetContentType()

	// Assert
	assert.Equal(contentType, "application/json")
}
