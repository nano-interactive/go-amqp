package serializer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type message struct {
	Name string `json:"name"`
}

var serializer Serializer[message] = JsonSerializer[message]{}

func TestSerializerSuccess(t *testing.T) {
	// Arrange
	assert := require.New(t)
	data := message{Name: "Test"}

	// Act
	json, err := serializer.Marshal(data)

	// Assert
	assert.NoError(err)
	assert.NotNil(json)
	assert.JSONEq(string(json), `{"name": "Test"}`)
}

func TestSerializerEmpty(t *testing.T) {
	// Arrange
	assert := require.New(t)
	data := message{Name: ""}

	// Act
	json, err := serializer.Marshal(data)

	// Assert
	assert.NoError(err)
	assert.NotNil(json)
	assert.JSONEq(string(json), `{"name": ""}`)
}

func TestDeSerializerSuccess(t *testing.T) {
	// Arrange
	assert := require.New(t)

	// Act
	data, err := serializer.Unmarshal([]byte(`{"name": "Test"}`))

	// Assert
	assert.NoError(err)
	assert.NotNil(data)
}

func TestDeSerializerFail(t *testing.T) {
	// Arrange
	assert := require.New(t)

	// Act
	data, err := serializer.Unmarshal([]byte(`{"name": false}`))

	// Assert
	assert.Error(err)
	assert.Empty(data)
}

func TestSerializerContentType(t *testing.T) {
	// Arrange
	assert := require.New(t)

	// Act
	contentType := serializer.GetContentType()

	// Assert
	assert.Equal(contentType, "application/json")
}
