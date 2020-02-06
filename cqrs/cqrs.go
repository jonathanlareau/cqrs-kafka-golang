package cqrs

import (
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Entity Entity in cqrs	
type Entity interface {
	Unmarshal(kafka.Message)
	String() string
}

// UUID Generate a uuid
func UUID() string {
	id := uuid.New()
	return id.String()
}