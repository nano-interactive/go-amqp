package serializer

type Serializer[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte) (T, error)
	GetContentType() string
}
