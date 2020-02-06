package main

import (
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"encoding/json"
	"time"
	cqrs "github.com/jonathanlareau/cqrs-kafka-golang/cqrs"
	"fmt"
	"net"
	"log"
	"strings"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy")

// CreateUser 
func CreateUser(kafkaMsg kafka.Message) {
	user := Unmarshal(kafkaMsg) 
	fmt.Println("Create User = "+ user.String())
}

// ReadUser 
func ReadUser(id int64) {
	fmt.Sprintf("REad Id = %d", id)
}

// UpdateUser 
func UpdateUser(kafkaMsg kafka.Message) {
	user := Unmarshal(kafkaMsg) 
	fmt.Println("Update User = "+ user.String())
}

// DeleteUser 
func DeleteUser(kafkaMsg kafka.Message) {
	user := Unmarshal(kafkaMsg) 
	fmt.Println("Delete User = "+ user.String())
}

func Unmarshal(kafkaMsg kafka.Message) pb.User {
	var user pb.User
	err := json.Unmarshal(kafkaMsg.Value, &user)
	if err != nil {
		fmt.Errorf("Error while Unmarshal a message: %v", err)
	}
	return user
}

type server struct{}

type UserEntity struct{
    User pb.User
}

var count = 0
var createUserPublisher Publisher = nil
var updateUserPublisher Publisher = nil
var deleteUserPublisher Publisher = nil
var createUserConsumer Consumer = nil
var updateUserConsumer Consumer = nil
var deleteUserConsumer Consumer = nil

func main() {
	lis, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatalf("Failed to listen on port 3000:  %v", err)
	}
	var (
		brokers = "kafka-zookeeper:9092"
		createUserTopic   = "user-create"
		updateUserTopic   = "user-update"
		deleteUserTopic   = "user-delete"
	)

	createUserPublisher = NewPublisher(strings.Split(brokers, ","), createUserTopic)
	updateUserPublisher = NewPublisher(strings.Split(brokers, ","), updateUserTopic)
	deleteUserPublisher = NewPublisher(strings.Split(brokers, ","), deleteUserTopic)
	createUserConsumer = NewConsumer(strings.Split(brokers, ","), createUserTopic)
	updateUserConsumer = NewConsumer(strings.Split(brokers, ","), updateUserTopic)
	deleteUserConsumer = NewConsumer(strings.Split(brokers, ","), deleteUserTopic)

	go func() {
		createUserConsumer.Read(context.Background(),func(kafkaMsg kafka.Message){CreateUser(kafkaMsg)})
	}()
	go func() {
		updateUserConsumer.Read(context.Background(),func(kafkaMsg kafka.Message){UpdateUser(kafkaMsg)})
	}()
	go func() {
		deleteUserConsumer.Read(context.Background(),func(kafkaMsg kafka.Message){DeleteUser(kafkaMsg)})
	}()

	s := grpc.NewServer()
	pb.RegisterUserServiceServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *server) CreateUser(cxt context.Context, user *pb.User) (*pb.Result, error) {
	log.Println("CreateUser")

	if err := createUserPublisher.Publish(context.Background(), user); err != nil {
		log.Fatal(err)
	}

	result := &pb.Result{Code: 1, Msg: "allo"}
	count++
	return result, nil
}

func (s *server) ReadUser(cxt context.Context, id *pb.Id) (*pb.User, error) {
	log.Println("CreateUser")
	user := &pb.User{UserId: 11, FirstName: "Jonathan", LastName: "Lareau", Age: 44}
	count++
	return user, nil
}

func (s *server) UpdateUser(cxt context.Context,  user *pb.User) (*pb.Result, error) {
	log.Println("UpdateUser")

	if err := updateUserPublisher.Publish(context.Background(), user); err != nil {
		log.Fatal(err)
	}

	result := &pb.Result{Code: 1, Msg: "allo"}
	count++
	return result, nil
}

func (s *server) DeleteUser(cxt context.Context, id *pb.Id) (*pb.Result, error) {
	log.Println("deleteUser")

	if err := deleteUserPublisher.Publish(context.Background(), id); err != nil {
		log.Fatal(err)
	}

	result := &pb.Result{Code: 1, Msg: "allo"}
	count++
	return result, nil
}

type consumer struct {
	reader *kafka.Reader
}

// Consumer read from the stream
type Consumer interface {
	Read(ctx context.Context, crud func(kafkaMsg kafka.Message))
}

func NewConsumer(brokers []string, topic string) Consumer {
	c := kafka.ReaderConfig{
		Brokers:         brokers,               // from:9092
		Topic:           topic,                 // chat
		MinBytes:        10e3,                  // 10 KB
		MaxBytes:        10e6,                  // 10 MB
		MaxWait:         10 * time.Millisecond, // 10 Millisecond
		ReadLagInterval: -1,
		GroupID:         cqrs.UUID(),
		StartOffset:     kafka.LastOffset,
	}
	return &consumer{kafka.NewReader(c)}
}


func (c *consumer) Read(ctx context.Context, crud func(kafkaMsg kafka.Message)) {
	defer c.reader.Close()
	for {
		kafkaMsg, err := c.reader.ReadMessage(ctx)
//		kafkaMsg = "sd"
		if err != nil {
			fmt.Errorf("Error while reading a message: %v", err)
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
		ClientID: topic+"-KAFKACHATCLIENID001",
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

	key := cqrs.UUID()
	return kafka.Message{
		Key:   []byte(key),
		Value: m,
	}, nil
}
