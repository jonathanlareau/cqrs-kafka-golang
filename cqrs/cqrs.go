package cqrs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
)

// UUID Generate a uuid
func UUID() string {
	id := uuid.New()
	return id.String()
}

type consumer struct {
	reader *kafka.Reader
}

// Consumer read from the stream
type Consumer interface {
	Read(ctx context.Context, crud func(kafkaMsg kafka.Message))
}

// NewConsumer create a generic consumer
func NewConsumer(brokers []string, topic string) Consumer {
	c := kafka.ReaderConfig{
		Brokers:         brokers,               // from:9092
		Topic:           topic,                 // chat
		MinBytes:        10e3,                  // 10 KB
		MaxBytes:        10e6,                  // 10 MB
		MaxWait:         10 * time.Millisecond, // 10 Millisecond
		ReadLagInterval: -1,
		GroupID:         UUID(),
		StartOffset:     kafka.LastOffset,
	}
	return &consumer{kafka.NewReader(c)}
}

func (c *consumer) Read(ctx context.Context, crud func(kafkaMsg kafka.Message)) {
	defer c.reader.Close()
	for {
		kafkaMsg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			s := fmt.Errorf("Error while reading a message: %v", err)
			fmt.Println(s)
			continue
		}
		crud(kafkaMsg)
	}
}

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
		ClientID: topic + "-KAFKACHATCLIENID001",
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
