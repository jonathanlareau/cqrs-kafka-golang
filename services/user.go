package service

import (
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	utils "github.com/jonathanlareau/cqrs-kafka-golang/utils"
	"fmt"
	"net"
	"log"
	"strings"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// CreateUser 
func CreateUser(user pb.User) {
	fmt.Println("User = "+ user.String())
}

// ReadUser 
func ReadUser(id int64) {
	fmt.Sprintf("Id = %d", id)
}

// UpdateUser 
func UpdateUser(user pb.User) {
	fmt.Println("User = "+ user.String())
}

// DeleteUser 
func DeleteUser(id int64) {
	fmt.Sprintf("Id = %d", id)
}

type server struct{}

var count = 0
var createUserPublisher utils.Publisher = nil
var updateUserPublisher utils.Publisher = nil
var deleteUserPublisher utils.Publisher = nil

func main() {
	lis, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatalf("Failed to listen on port 3000:  %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterUserServiceServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	var (
		brokers = "kafka-zookeeper:9092"
		createUserTopic   = "user-create"
		updateUserTopic   = "update-create"
		deleteUserTopic   = "delete-create"
	)

	createUserPublisher = utils.NewPublisher(strings.Split(brokers, ","), createUserTopic)
	updateUserPublisher = utils.NewPublisher(strings.Split(brokers, ","), updateUserTopic)
	deleteUserPublisher = utils.NewPublisher(strings.Split(brokers, ","), deleteUserTopic)

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

