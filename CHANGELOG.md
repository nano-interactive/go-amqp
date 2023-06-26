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

## Features

* Added `testing.AssertAMQPMessageCount`
* Added `testing.PublishConfig` to declare AMQP Exchange params
* Added missing `ExchangeTypeHeader` to publisher
## Bugfixes

# v1.1.0

* Retry Message Handler