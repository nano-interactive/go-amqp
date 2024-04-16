package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/consumer"
	"github.com/nano-interactive/go-amqp/v3/publisher"
)

func GetAMQPConnection(tb testing.TB, cfg connection.Config) (*amqp091.Connection, *amqp091.Channel) {
	tb.Helper()

	if cfg.Channels == 0 {
		cfg.Channels = connection.DefaultConfig.Channels
	}

	if cfg.Vhost == "" {
		cfg.Vhost = connection.DefaultConfig.Vhost
	}

	if cfg.Host == "" {
		cfg.Host = connection.DefaultConfig.Host
	}

	if cfg.Port == 0 {
		cfg.Port = connection.DefaultConfig.Port
	}

	if cfg.User == "" {
		cfg.User = connection.DefaultConfig.User
	}

	if cfg.Password == "" {
		cfg.Password = connection.DefaultConfig.Password
	}

	if cfg.ReconnectInterval == 0 {
		cfg.ReconnectInterval = connection.DefaultConfig.ReconnectInterval
	}

	if cfg.ReconnectRetry == 0 {
		cfg.ReconnectRetry = connection.DefaultConfig.ReconnectRetry
	}

	connectionURI := fmt.Sprintf(
		"amqp://%s:%s@%s",
		cfg.User,
		cfg.Password,
		net.JoinHostPort(cfg.Host, strconv.FormatInt(int64(cfg.Port), 10)),
	)

	properties := amqp091.NewConnectionProperties()
	properties.SetClientConnectionName("testing_connection_name")

	config := amqp091.Config{
		Vhost:      cfg.Vhost,
		ChannelMax: cfg.Channels,
		Properties: properties,
		Dial:       amqp091.DefaultDial(10 * time.Second),
	}

	conn, err := amqp091.DialConfig(connectionURI, config)
	if err != nil {
		tb.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err = ch.Close(); err != nil {
			tb.Logf("error closing channel: %v", err)
			return
		}

		if err = conn.Close(); err != nil {
			tb.Logf("error closing connection: %v", err)
			return
		}
	})

	return conn, ch
}

type PublisherConfig struct {
	Name   string
	Config publisher.ExchangeDeclare
}

type ConsumerConfig struct {
	Name   string
	Config consumer.QueueDeclare
}

type QueueExchangeMapping struct {
	t       testing.TB
	channel *amqp091.Channel
	mapping map[string][]string
}

func NewMappings(tb testing.TB, config ...connection.Config) *QueueExchangeMapping {
	tb.Helper()
	cfg := connection.DefaultConfig

	if len(config) > 0 {
		cfg = config[0]
	}

	_, channel := GetAMQPConnection(tb, cfg)

	return &QueueExchangeMapping{
		mapping: make(map[string][]string),
		channel: channel,
		t:       tb,
	}
}

func (q *QueueExchangeMapping) declareExchange(exchangeCfg PublisherConfig) {
	kind := amqp091.ExchangeFanout

	if exchangeCfg.Config.Type.String() != "" {
		kind = exchangeCfg.Config.Type.String()
	}

	durable := true
	if !exchangeCfg.Config.Durable {
		durable = false
	}

	args := make(amqp091.Table)

	if exchangeCfg.Config.Args != nil {
		args = exchangeCfg.Config.Args
	}

	err := q.channel.ExchangeDeclare(
		exchangeCfg.Name,
		kind,
		durable,
		exchangeCfg.Config.AutoDelete,
		exchangeCfg.Config.Internal,
		exchangeCfg.Config.NoWait,
		args,
	)
	if err != nil {
		q.t.Fatal(err)
	}

	q.t.Cleanup(func() {
		_ = q.channel.ExchangeDelete(exchangeCfg.Name, false, false)
	})
}

func (q *QueueExchangeMapping) declareQueue(queue ConsumerConfig, exchangeName, exchangeRoutingKey string) {
	durable := true
	if !queue.Config.Durable {
		durable = false
	}

	_, err := q.channel.QueueDeclare(
		queue.Config.QueueName,
		durable,
		queue.Config.AutoDelete,
		queue.Config.Exclusive,
		queue.Config.NoWait,
		nil,
	)
	if err != nil {
		q.t.Fatal(err)
	}

	_ = q.channel.QueueBind(
		queue.Config.QueueName,
		exchangeRoutingKey,
		exchangeName,
		false,
		nil,
	)

	q.t.Cleanup(func() {
		_ = q.channel.QueueUnbind(queue.Config.QueueName, exchangeRoutingKey, exchangeName, nil)
		_, _ = q.channel.QueueDelete(queue.Config.QueueName, false, false, false)
	})
}

func (q *QueueExchangeMapping) AddMapping(exchange string, queue ...string) *QueueExchangeMapping {
	q.t.Helper()

	if len(queue) == 0 {
		q.t.Fatal("no queues provided")
	}

	cfg := make([]ConsumerConfig, len(queue))

	for i, v := range queue {
		cfg[i] = ConsumerConfig{
			Name: v,
			Config: consumer.QueueDeclare{
				QueueName:  v,
				Durable:    true,
				AutoDelete: false,
				Exclusive:  false,
				NoWait:     false,
			},
		}
	}

	return q.AddMappings(
		PublisherConfig{
			Name: exchange,
			Config: publisher.ExchangeDeclare{
				Type:       publisher.ExchangeTypeFanout,
				Durable:    true,
				AutoDelete: false,
				Internal:   false,
				NoWait:     false,
				Args:       nil,
			},
		},
		cfg,
	)
}

func (q *QueueExchangeMapping) AddMappingConfig(exchangeCfg PublisherConfig, queue ConsumerConfig) *QueueExchangeMapping {
	return q.AddMappings(exchangeCfg, []ConsumerConfig{queue})
}

func (q *QueueExchangeMapping) AddMappings(exchangeCfg PublisherConfig, queue []ConsumerConfig) *QueueExchangeMapping {
	exchangeCfg.Name = fmt.Sprintf("%s-%s", exchangeCfg.Name, q.t.Name())

	q.declareExchange(exchangeCfg)

	for _, queueCfg := range queue {
		queueCfg.Config.QueueName = fmt.Sprintf("%s-%s", queueCfg.Config.QueueName, q.t.Name())

		q.declareQueue(queueCfg, exchangeCfg.Name, exchangeCfg.Config.RoutingKey)

		q.mapping[exchangeCfg.Name] = append(q.mapping[exchangeCfg.Name], queueCfg.Config.QueueName)
	}

	return q
}

func (q QueueExchangeMapping) Exchange(prefix string) string {
	for exchangeName := range q.mapping {
		if strings.HasPrefix(exchangeName, prefix+"-") {
			return exchangeName
		}
	}

	return ""
}

func (q QueueExchangeMapping) Queue(prefix string) string {
	for _, queues := range q.mapping {
		for _, queueName := range queues {
			if strings.HasPrefix(queueName, prefix+"-") {
				return queueName
			}
		}
	}

	return ""
}

func AssertAMQPMessageCount[T any](
	tb testing.TB,
	queueName string,
	expectedCount int,
	cfg connection.Config,
	duration ...time.Duration,
) []T {
	tb.Helper()

	messages := ConsumeAMQPMessages[T](tb, queueName, cfg, duration...)

	if len(messages) != expectedCount {
		tb.Fatalf("expected %d messages, got %d", expectedCount, len(messages))
	}

	return messages
}

func ConsumeAMQPMessages[T any](
	tb testing.TB,
	queueName string,
	cfg connection.Config,
	duration ...time.Duration,
) []T {
	tb.Helper()
	messages := make([]T, 0, 10)

	_, channel := GetAMQPConnection(tb, cfg)

	ch, err := channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		tb.Fatal(err)
	}

	ctx := context.Background()

	if len(duration) > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration[0])
		tb.Cleanup(cancel)
	}

	for {
		select {
		case d, more := <-ch:
			if !more {
				return messages
			}

			if err = d.Ack(false); err != nil {
				tb.Fatal(err)
			}

			var data T

			if err = json.Unmarshal(d.Body, &data); err != nil {
				tb.Fatal(err)
			}

			messages = append(messages, data)
		case <-ctx.Done():
			return messages
		}
	}
}

type PublishConfig struct {
	Marshal     func(any) ([]byte, error)
	RoutingKey  string
	ContentType string
	Duration    time.Duration
}

func PublishAMQPMessage(
	tb testing.TB,
	exchange string,
	msg any,
	conn connection.Config,
	config ...PublishConfig,
) {
	tb.Helper()

	cfg := PublishConfig{Duration: 0, RoutingKey: "", Marshal: json.Marshal, ContentType: "application/json"}
	if len(config) > 0 {
		if config[0].Marshal == nil {
			config[0].Marshal = json.Marshal
		}

		if config[0].ContentType == "" {
			config[0].ContentType = "application/json"
		}

		cfg = config[0]
	}

	message, err := cfg.Marshal(msg)
	if err != nil {
		tb.Fatal(err)
	}

	_, channel := GetAMQPConnection(tb, conn)
	ctx := context.Background()

	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		tb.Cleanup(cancel)
	}

	err = channel.PublishWithContext(
		ctx,
		exchange,
		cfg.RoutingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType:  cfg.ContentType,
			DeliveryMode: amqp091.Persistent,
			Timestamp:    time.Now(),
			Body:         message,
		},
	)
	if err != nil {
		tb.Fatal(err)
	}

	tb.Logf("Published message")
}
