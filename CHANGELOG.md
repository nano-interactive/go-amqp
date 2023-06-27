# 2.0.3
## Bufix

* Fixing issue with testing publish

# 2.0.2
## Bufix

* Fix deref in type check

# 2.0.1

## Bufix

* `publisher.Pub[T]` interface matches the `publisher.Publisher[T]`

# v2.0.0

## Breaking

* Publisher now accepts `exchangeName string` as required parameter to `publisher.New()`
* Testing package renamed from `testing_utils` to `testing`
* Removed `testing.SetupAmqp` in favour of `NewMappings`
* `ConsumeAMQPMessages` no longer accepts `*amqp091.Channel`
* `ConsumeAMQPMessages` no longer accepts `context.Context`
* `ConsumeAMQPMessages` has `time.Duration` as optional parameter
* `PublishAMQPMessage` no longer accepts `*amqp091.Channel`
* `PublishAMQPMessage` no longer accepts `context.Context`
* `Logger` moved to `logging` package
* `publisher.Pub[T]` has additional parameter for config -> future Use

## Features

* Added `testing.AssertAMQPMessageCount`
* Added `testing.PublishConfig` to declare AMQP Exchange params
* Added missing `ExchangeTypeHeader` to publisher

## Bugfixes

* When connecting for the first time, `connection.Connection` respects `connection.Config.ReconnectRetry` and `connection.Config.ReconnectInterval`
* Deadlock on `publisher` when connecting more than once -> locked mutex

# v1.1.0

* Retry Message Handler