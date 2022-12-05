package amqp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Logger interface {
	Error(string, ...any)
}

type ConnectionParams struct {
	Host     string
	User     string
	Password string
	Vhost    string
	Port     int
}

type QueueConfig struct {
	ConnectionNamePrefix string
	Workers              int
	PrefetchCount        int
	PrefetchSize         int
	ConnectionParams     ConnectionParams
	RetryCount           int
	SleepInterval        time.Duration
}

type queue[T Message] struct {
	logger     Logger
	ctx        context.Context
	cancel     context.CancelFunc
	cfg        *QueueConfig
	connection *amqp091.Connection
	wg         *sync.WaitGroup
	handler    Handler[T]
}

func shouldReconnect(amqpErr *amqp091.Error) bool {
	return amqpErr.Code == amqp091.ConnectionForced ||
		amqpErr.Code == amqp091.ResourceLocked ||
		amqpErr.Code == amqp091.PreconditionFailed ||
		amqpErr.Code == amqp091.ChannelError ||
		amqpErr.Code == amqp091.ResourceError
}

func newQueue[T Message](base context.Context, logger Logger, cfg *QueueConfig, handler Handler[T]) (*queue[T], error) {
	ctx, cancel := context.WithCancel(base)
	properties := amqp091.NewConnectionProperties()
	var msg T
	properties.SetClientConnectionName(fmt.Sprintf("%s_%s", cfg.ConnectionNamePrefix, msg.GetQueueName()))

	if cfg.RetryCount < 1 {
		cfg.RetryCount = 10
	}

	if cfg.SleepInterval == 0 {
		cfg.SleepInterval = 1 * time.Second
	}

	config := amqp091.Config{
		Properties: properties,
		Heartbeat:  1 * time.Second,
		ChannelMax: cfg.Workers,
	}

	wg := &sync.WaitGroup{}
	connectionURI := fmt.Sprintf("amqp://%s:%s@%s:%d%s", cfg.ConnectionParams.User, cfg.ConnectionParams.Password, cfg.ConnectionParams.Host, cfg.ConnectionParams.Port, cfg.ConnectionParams.Vhost)

	conn, err := amqp091.DialConfig(connectionURI, config)
	if err != nil {
		cancel()
		return nil, err
	}

	queue := &queue[T]{
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		connection: conn,
		cfg:        cfg,
		wg:         wg,
		handler:    handler,
	}

	ch := make(chan *amqp091.Error)
	conn.NotifyClose(ch)
	go handleErrors(base, queue, ch, connectionURI, config)

	return queue, nil
}

func (q *queue[T]) Listen() error {
	q.wg.Add(q.cfg.Workers)
	var msg T

	amqpChannel, err := q.connection.Channel()
	if err != nil {
		return err
	}

	if _, err = amqpChannel.QueueDeclare(msg.GetQueueName(), true, false, false, false, nil); err != nil {
		return err
	}

	if err := amqpChannel.Close(); err != nil {
		return err
	}

	for i := 0; i < q.cfg.Workers; i++ {
		channel, err := q.connection.Channel()
		if err != nil {
			return err
		}

		go listener(q.ctx, q.wg, channel, q.cfg, q.handler, q.logger, msg.GetQueueName())
	}

	return nil
}

func (q *queue[T]) Close() error {
	q.cancel()
	q.wg.Wait()

	return q.connection.Close()
}
