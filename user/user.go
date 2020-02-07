package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/jonathanlareau/cqrs-kafka-golang/cqrs"
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// CreateUser verew
func CreateUser(kafkaMsg kafka.Message) {
	user := Unmarshal(kafkaMsg)
	fmt.Println("Create User = " + user.String())
	SetInRedis(user)
}

// ReadUser sfsdf
func ReadUser(id int64) {
	s := fmt.Sprintf("REad Id = %d", id)
	fmt.Println(s)
}

// UpdateUser fsafdsd
func UpdateUser(kafkaMsg kafka.Message) {
	user := Unmarshal(kafkaMsg)
	fmt.Println("Update User = " + user.String())
}

// DeleteUser fdsfd
func DeleteUser(kafkaMsg kafka.Message) {
	user := Unmarshal(kafkaMsg)
	fmt.Println("Delete User = " + user.String())
}

// Unmarshal erter
func Unmarshal(kafkaMsg kafka.Message) pb.User {
	var user pb.User
	err := json.Unmarshal(kafkaMsg.Value, &user)
	if err != nil {
		s := fmt.Errorf("Error while Unmarshal a message: %v", err)
		fmt.Println(s)
	}
	return user
}

type server struct{}

var count = 0
var createUserPublisher cqrs.Publisher = nil
var updateUserPublisher cqrs.Publisher = nil
var deleteUserPublisher cqrs.Publisher = nil
var createUserConsumer cqrs.Consumer = nil
var updateUserConsumer cqrs.Consumer = nil
var deleteUserConsumer cqrs.Consumer = nil
var redisClient redis.Conn = nil

func main() {
	var err error
	redisClient, err = redis.Dial("tcp", "192.168.99.100:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer redisClient.Close()

	lis, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatalf("Failed to listen on port 3000:  %v", err)
	}
	var (
		brokers         = "kafka-zookeeper:9092"
		createUserTopic = "user-create"
		updateUserTopic = "user-update"
		deleteUserTopic = "user-delete"
	)

	createUserPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), createUserTopic)
	updateUserPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), updateUserTopic)
	deleteUserPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), deleteUserTopic)
	createUserConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), createUserTopic)
	updateUserConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), updateUserTopic)
	deleteUserConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), deleteUserTopic)

	go func() {
		createUserConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { CreateUser(kafkaMsg) })
	}()
	go func() {
		updateUserConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { UpdateUser(kafkaMsg) })
	}()
	go func() {
		deleteUserConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { DeleteUser(kafkaMsg) })
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

func (s *server) UpdateUser(cxt context.Context, user *pb.User) (*pb.Result, error) {
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

// SetInRedis fsdfd
func SetInRedis(user pb.User) {
	marsh, errMarsh := json.Marshal(user)
	if errMarsh != nil {
		s := fmt.Errorf("Error while Unmarshal a message: %v", errMarsh)
		fmt.Println(s)
	}
	_, errRedis := redisClient.Do("HMSET", "user-"+string(user.UserId), "user", marsh)
	if errRedis != nil {
		s := fmt.Errorf("Error while Unmarshal a message: %v", errRedis)
		fmt.Println(s)
	}
}

// GetFromRedis fsdfd
func GetFromRedis(key int64) pb.User {
	var newUser []byte
	_, errRedis := redisClient.Do("HMGET", "user-"+string(key), "user", &newUser)
	if errRedis != nil {
		s := fmt.Errorf("Error while Unmarshal a message: %v", errRedis)
		fmt.Println(s)
	}
	var user pb.User
	err := json.Unmarshal(newUser, &user)
	if err != nil {
		s := fmt.Errorf("Error while Unmarshal a message: %v", err)
		fmt.Println(s)
	}
	return user
}
