package amqp

type Logger interface {
	Error(string, ...any)
}

type EmptyLogger struct{}

func (d EmptyLogger) Error(msg string, args ...any) {}
