package util

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
)

type publisher struct {
	writer *kafka.Writer
}

// Publisher write a message into a stream
type Publisher interface {
	// Publish write a message into a stream
	Publish(ctx context.Context, payload interface{}) error
}

// NewPublisher create an instance of publisher
func NewPublisher(brokers []string, topic string) Publisher {
	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: "KAFKACHATCLIENID001",
	}

	c := kafka.WriterConfig{
		Brokers:          brokers, // from :9092
		Topic:            topic,   // chat
		Balancer:         &kafka.LeastBytes{},
		Dialer:           dialer,
		WriteTimeout:     10 * time.Second, // 10 sec
		ReadTimeout:      10 * time.Second, // 10 sec
		CompressionCodec: snappy.NewCompressionCodec(),
	}

	return &publisher{kafka.NewWriter(c)}
}

func (p *publisher) Publish(ctx context.Context, payload interface{}) error {
	message, err := p.encodeMessage(payload)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, message)
}

func (p *publisher) encodeMessage(payload interface{}) (kafka.Message, error) {
	m, err := json.Marshal(payload)
	if err != nil {
		return kafka.Message{}, err
	}

	key := UUID()
	return kafka.Message{
		Key:   []byte(key),
		Value: m,
	}, nil
}
