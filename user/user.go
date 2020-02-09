package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/jackc/pgx/v4"
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
	CreateUserInDb(user)
	SetInRedis(user)
}

// ReadUser sfsdf
func ReadUser(id int64) pb.User {
	s := fmt.Sprintf("REad Id = %d", id)
	fmt.Println(s)
	user := GetInRedis(id)
	return user
}

// UpdateUser fsafdsd
func UpdateUser(kafkaMsg kafka.Message) {
	user := Unmarshal(kafkaMsg)
	fmt.Println("Update User = " + user.String())
	UpdateUserInDb(user)
	SetInRedis(user)
}

// DeleteUser fdsfd
func DeleteUser(kafkaMsg kafka.Message) {
	userID := UnmarshalID(kafkaMsg)
	fmt.Println("Delete User = " + userID.String())
	DeleteUserInDb(userID.Id)
	RemoveInRedis(userID.Id)
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

// UnmarshalID erter
func UnmarshalID(kafkaMsg kafka.Message) pb.Id {
	var userID pb.Id
	err := json.Unmarshal(kafkaMsg.Value, &userID)
	if err != nil {
		s := fmt.Errorf("Error while Unmarshal a message: %v", err)
		fmt.Println(s)
	}
	return userID
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
var dbConn *pgx.Conn

func main() {
	var err error
	redisClient, err = redis.Dial("tcp", "192.168.99.100:6379")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to Redis")
	defer redisClient.Close()
	dbConn, err = pgx.Connect(context.Background(), "postgresql://postgres:password@192.168.99.100:5432/cqrs")
	fmt.Println("Connected to Postgresql")
	defer dbConn.Close(context.Background())

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
	msg := "User will be created"
	if err := createUserPublisher.Publish(context.Background(), user); err != nil {
		log.Fatal(err)
		msg = "Error on publishing for creation"
	}
	result := &pb.Result{Code: 0, Msg: msg}
	return result, nil
}

func (s *server) ReadUser(cxt context.Context, id *pb.Id) (*pb.User, error) {
	log.Println("ReadUser server")
	log.Println(id)
	user := ReadUser(id.Id)
	fmt.Println(user)
	return &user, nil
}

func (s *server) UpdateUser(cxt context.Context, user *pb.User) (*pb.Result, error) {
	log.Println("UpdateUser")
	msg := "User will be updated"
	if err := updateUserPublisher.Publish(context.Background(), user); err != nil {
		log.Fatal(err)
		msg = "Error on publishing for creation"
	}
	result := &pb.Result{Code: 0, Msg: msg}
	return result, nil
}

func (s *server) DeleteUser(cxt context.Context, id *pb.Id) (*pb.Result, error) {
	log.Println("DeleteUser server")
	log.Println(id)
	msg := "User will be updated"
	if err := deleteUserPublisher.Publish(context.Background(), id); err != nil {
		log.Fatal(err)
		msg = "Error on publishing for creation"
	}
	result := &pb.Result{Code: 0, Msg: msg}
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

// GetInRedis fsdfd
func GetInRedis(key int64) pb.User {
	fmt.Println("GetInRedis")
	fmt.Println(key)
	var newUser []byte
	_, errRedis := redisClient.Do("HMGET", "user-"+string(key), "user", &newUser)
	fmt.Println(newUser)
	if newUser == nil {
		fmt.Println("user not found")
		user := ReadUserInDb(key)
		fmt.Println(user)
		SetInRedis(user)
		return user
	}
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

// RemoveInRedis fsdfd
func RemoveInRedis(key int64) {
	_, errRedis := redisClient.Do("HDEL", "user-"+string(key), "user")
	if errRedis != nil {
		s := fmt.Errorf("Error while Unmarshal a message: %v", errRedis)
		fmt.Println(s)
	}
}

// ReadNextUserID Not Prod Ready... just for test
func ReadNextUserID() int64 {
	var userID int64
	err := dbConn.QueryRow(context.Background(), "select max(userid) from cqrs_user").Scan(&userID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(userID)
	userID++
	return userID
}

// ReadUserInDb erwr
func ReadUserInDb(userID int64) pb.User {
	var dbuser pb.User
	fmt.Println("Read db")
	err := dbConn.QueryRow(context.Background(), "select userid,firstname,lastname,age from cqrs_user where userid=$1", userID).Scan(&dbuser.UserId, &dbuser.FirstName, &dbuser.LastName, &dbuser.Age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(dbuser)
	return dbuser
}

// CreateUserInDb sfsdfsd
func CreateUserInDb(user pb.User) pb.User {
	fmt.Println("create")
	userID := ReadNextUserID()
	value, err := dbConn.Exec(context.Background(), "insert into cqrs_user (userid, firstname, lastname, age) values( $1,$2,$3,$4) ", userID, user.FirstName, user.LastName, user.Age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(value)
	fmt.Println(userID)
	return ReadUserInDb(userID)
}

// UpdateUserInDb sfdsf
func UpdateUserInDb(user pb.User) pb.User {
	fmt.Println("update")
	value, err := dbConn.Exec(context.Background(), "update cqrs_user set firstname = $2, lastname = $3, age = $4 where userid = $1 ", user.UserId, user.FirstName, user.LastName, user.Age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(value)
	fmt.Println(user)
	return ReadUserInDb(user.UserId)
}

// DeleteUserInDb dsfsd
func DeleteUserInDb(userID int64) {
	fmt.Println("delete db")
	fmt.Println(userID)
	value, err := dbConn.Exec(context.Background(), "delete from cqrs_user where userid = $1 ", userID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(value)
	fmt.Println(userID)
}
