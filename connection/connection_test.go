package connection

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestConnectionBasic(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test basic connection
	conn, err := New(ctx, DefaultConfig, Events{
		OnConnectionReady: func(ctx context.Context, conn *amqp091.Connection) error {
			return nil
		},
	})
	assert.NoError(err)
	assert.NotNil(conn)

	// Test connection state
	assert.Equal(StateConnected, conn.getState())

	// Test connection close
	assert.NoError(conn.Close())
	assert.Equal(StateClosing, conn.getState())
}

func TestConnectionReconnect(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var errorReceived bool
	var lastError error

	// Test connection with reconnection
	conn, err := New(ctx, DefaultConfig, Events{
		OnConnectionReady: func(ctx context.Context, conn *amqp091.Connection) error {
			return nil
		},
		OnError: func(err error) {
			errorReceived = true
			lastError = err
		},
	})
	assert.NoError(err)
	assert.NotNil(conn)

	// Simulate connection loss
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second)
		conn.connectionDispose()
	}()

	// Wait for reconnection
	wg.Wait()
	time.Sleep(3 * time.Second)

	// Check if reconnected
	assert.Equal(StateConnected, conn.getState())
	assert.True(errorReceived, "should have received an error during reconnection")
	assert.NotNil(lastError, "last error should not be nil")
	assert.NoError(conn.Close())
}

func TestConnectionErrorHandling(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var lastError error

	// Test connection with error handling
	conn, err := New(ctx, DefaultConfig, Events{
		OnConnectionReady: func(ctx context.Context, conn *amqp091.Connection) error {
			return errors.New("test error")
		},
		OnError: func(err error) {
			lastError = err
		},
	})
	assert.Error(err)
	assert.Nil(conn)
	assert.NotNil(lastError)
	assert.Equal("test error", lastError.Error())
}

func TestConnectionConcurrent(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	conn, err := New(ctx, DefaultConfig, Events{
		OnConnectionReady: func(ctx context.Context, conn *amqp091.Connection) error {
			return nil
		},
	})
	assert.NoError(err)
	assert.NotNil(conn)

	// Test concurrent operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Simulate some operations
			time.Sleep(100 * time.Millisecond)
		}()
	}

	wg.Wait()
	assert.NoError(conn.Close())
}

func TestConnectionClose(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := New(ctx, DefaultConfig, Events{
		OnConnectionReady: func(ctx context.Context, conn *amqp091.Connection) error {
			return nil
		},
	})
	assert.NoError(err)
	assert.NotNil(conn)

	// Test multiple close calls
	for i := 0; i < 5; i++ {
		assert.NoError(conn.Close())
	}

	// Test operations after close
	assert.Equal(StateClosing, conn.getState())
}

func TestConnectionBlocked(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var blockedReceived bool
	var wg sync.WaitGroup

	conn, err := New(ctx, DefaultConfig, Events{
		OnConnectionReady: func(ctx context.Context, conn *amqp091.Connection) error {
			return nil
		},
		OnError: func(err error) {
			if _, ok := err.(*BlockedError); ok {
				blockedReceived = true
			}
		},
	})
	assert.NoError(err)
	assert.NotNil(conn)

	// Simulate blocked state
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)
		// Simulate blocked state
		if conn.onError != nil {
			conn.onError(&BlockedError{
				Blocked: amqp091.Blocking{
					Active: true,
					Reason: "test",
				},
			})
		}
	}()

	wg.Wait()
	time.Sleep(1 * time.Second)

	assert.True(blockedReceived)
	assert.NoError(conn.Close())
}

func TestConnectionContextCancel(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	conn, err := New(ctx, DefaultConfig, Events{
		OnConnectionReady: func(ctx context.Context, conn *amqp091.Connection) error {
			return nil
		},
	})
	assert.NoError(err)
	assert.NotNil(conn)

	// Cancel context
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Connection should be in closing state
	assert.Equal(StateClosing, conn.getState())

	// Wait for cleanup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(conn.Close())
	}()

	// Wait for cleanup to complete
	wg.Wait()
}

func TestConnectionReconnectRetry(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	config := DefaultConfig
	config.ReconnectRetry = 3
	config.ReconnectInterval = 1 * time.Second

	var reconnectAttempts int
	var lastError error

	conn, err := New(ctx, config, Events{
		OnConnectionReady: func(ctx context.Context, conn *amqp091.Connection) error {
			reconnectAttempts++
			if reconnectAttempts < 3 {
				return errors.New("simulated error")
			}
			return nil
		},
		OnError: func(err error) {
			lastError = err
		},
	})
	assert.NoError(err)
	assert.NotNil(conn)
	assert.Equal(3, reconnectAttempts)
	assert.Equal(StateConnected, conn.getState())
	assert.NotNil(lastError)
	assert.Contains(lastError.Error(), "simulated error")

	assert.NoError(conn.Close())
}
