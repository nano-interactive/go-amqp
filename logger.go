package amqp

type Logger interface {
	Info(string, ...any)
	Error(string, ...any)
}

type EmptyLogger struct{}

func (d EmptyLogger) Error(msg string, args ...any) {}
func (d EmptyLogger) Info(msg string, args ...any) {}
