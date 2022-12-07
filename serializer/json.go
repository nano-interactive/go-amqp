package serializer

import "encoding/json"

type JsonSerializer[T any] struct{}

func (j JsonSerializer[T]) Marshal(v T) ([]byte, error) {
	data, err := json.Marshal(v)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (j JsonSerializer[T]) Unmarshal(data []byte) (T, error) {
	var value T

	if err := json.Unmarshal(data, &value); err != nil {
		return value, err
	}

	return value, nil
}

func (j JsonSerializer[T]) GetContentType() string {
	return "application/json"
}
