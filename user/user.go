/**
 * jonathan.lareau@gmail.com
 *
 * CQRS User Entity Microservice App
 *
 * Response to Facade Calls
 *
 **/
package main

import (
	"encoding/json"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jackc/pgx/v4"
	"github.com/jonathanlareau/cqrs-kafka-golang/cqrs"
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// For receive services fucntions calls
type server struct{}

// Global Variables
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

	// Initilize the connection with Postgresql
	redisClient, err = redis.Dial("tcp", "redis:6379")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to Redis")
	defer redisClient.Close()

	// Initilize the connection with Postgresql
	dbConn, err = pgx.Connect(context.Background(), "postgresql://postgres:password@postgresql:5432/cqrs")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to Postgresql")
	defer dbConn.Close(context.Background())

	// Const needed in Main Function
	const (
		brokers         = "kafka-zookeeper:9092"
		createUserTopic = "user-create"
		updateUserTopic = "user-update"
		deleteUserTopic = "user-delete"
	)

	// Create the Producers
	createUserPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), createUserTopic)
	updateUserPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), updateUserTopic)
	deleteUserPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), deleteUserTopic)

	// Create the consumers
	createUserConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), createUserTopic)
	updateUserConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), updateUserTopic)
	deleteUserConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), deleteUserTopic)

	// Initialize the Read in Consumers
	go func() {
		createUserConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { createUser(kafkaMsg) })
	}()
	go func() {
		updateUserConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { updateUser(kafkaMsg) })
	}()
	go func() {
		deleteUserConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { deleteUser(kafkaMsg) })
	}()

	// Prepare to Receive Call from GRPC Client
	userport := os.Getenv("USER_SERVICE_PORT")
	log.Printf("Application is running on : %s .....", userport)
	lis, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatalf("Failed to listen on port %s :  %v", userport, err)
	}
	s := grpc.NewServer()
	pb.RegisterUserServiceServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// Functions for the Producers

// Depending of your ID's generation strategies you need to adapt this function
// Response to CreateUser Service
func (s *server) CreateUser(cxt context.Context, user *pb.User) (*pb.User, error) {
	log.Println("CreateUser")
	if err := validate(user); err != nil {
		return user, err
	}
	newUser := pb.User{}
	dbuser := createUserInDb(newUser)
	user.UserId = dbuser.UserId
	if err := updateUserPublisher.Publish(context.Background(), user); err != nil {
		log.Fatal(err)
	}
	return user, nil
}

// Response to ReadUser Service
func (s *server) ReadUser(cxt context.Context, id *pb.Id) (*pb.User, error) {
	log.Println("ReadUser ", id)
	user := readUser(id.Id)
	return &user, nil
}

// Response to UpdateUser Service
func (s *server) UpdateUser(cxt context.Context, user *pb.User) (*pb.Result, error) {
	log.Println("UpdateUser ", user)
	if err := validate(user); err != nil {
		return &pb.Result{Code: 1, Msg: "None valid user"}, err
	}
	if err := updateUserPublisher.Publish(context.Background(), user); err != nil {
		log.Fatal(err)
	}
	return &pb.Result{Code: 0, Msg: "Update User Message Published"}, nil
}

// Response to DeleteUser Service
func (s *server) DeleteUser(cxt context.Context, id *pb.Id) (*pb.Result, error) {
	log.Println("DeleteUser", id)
	if err := deleteUserPublisher.Publish(context.Background(), id); err != nil {
		log.Fatal(err)
	}
	return &pb.Result{Code: 0, Msg: "Delete User Message Published"}, nil
}

// Response to CreateSyncUser Service
func (s *server) CreateSyncUser(cxt context.Context, user *pb.User) (*pb.User, error) {
	log.Println("CreateSyncUser")
	if err := validate(user); err != nil {
		return user, err
	}
	dbuser := createUserInDb(*user)
	return &dbuser, nil
}

// Response to ReadSyncUser Service
func (s *server) ReadSyncUser(cxt context.Context, id *pb.Id) (*pb.User, error) {
	log.Println("ReadUser ", id)
	user := readUserInDb(id.Id)
	return &user, nil
}

// Response to UpdateSyncUser Service
func (s *server) UpdateSyncUser(cxt context.Context, user *pb.User) (*pb.Result, error) {
	log.Println("UpdateUser ", user)
	if err := validate(user); err != nil {
		return &pb.Result{Code: 1, Msg: "None valid user"}, err
	}
	updateUserInDb(*user)
	return &pb.Result{Code: 0, Msg: "Update User Sync Mode"}, nil
}

// Response to DeleteUser Service
func (s *server) DeleteSyncUser(cxt context.Context, id *pb.Id) (*pb.Result, error) {
	log.Println("DeleteSyncUser", id)
	deleteUserInDb(id.Id)
	return &pb.Result{Code: 0, Msg: "Delete User Sync Mode"}, nil
}

// Response to GetUsers Service
func (s *server) GetUsers(*pb.User, pb.UserService_GetUsersServer) error {
	return nil
}

//Functions For Redis

// setInRedis Set a User Entry in Redis
func setInRedis(user pb.User) {
	log.Println("setInRedis ", user)
	marsh, errMarsh := json.Marshal(user)
	if errMarsh != nil {
		log.Printf("Error while Unmarshal a message: %v", errMarsh)
	}
	_, errRedis := redisClient.Do("SET", "user-"+strconv.FormatInt(user.UserId, 10), marsh)
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
}

// getInRedis Get a User Entry in Redis
func getInRedis(id int64) pb.User {
	log.Println("getInRedis ", id)
	newUser, errRedis := redis.Bytes(redisClient.Do("GET", "user-"+strconv.FormatInt(id, 10)))
	if newUser == nil {
		log.Printf("User not found in Redis")
		user := readUserInDb(id)
		setInRedis(user)
		return user
	}
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
	var user pb.User
	err := json.Unmarshal([]byte(newUser), &user)
	if err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return user
}

// removeInRedis Remove a User Entry in Redis
func removeInRedis(id int64) {
	log.Println("removeInRedis ", id)
	_, errRedis := redisClient.Do("DEL", "user-"+strconv.FormatInt(id, 10))
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
}

//Postgresql Functions

// createUserInDb Create User In Database
func createUserInDb(user pb.User) pb.User {
	log.Println("Create user id DB ", user)
	var userID int64
	err := dbConn.QueryRow(context.Background(), "insert into cqrs_user (firstname, lastname, age, updatedate, createdate) values( $1,$2,$3,$4,$5) RETURNING userid", user.FirstName, user.LastName, user.Age, time.Now(), time.Now()).Scan(&userID)
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(userID)
	return readUserInDb(userID)
}

// readUserInDb Read User In Database
func readUserInDb(userID int64) pb.User {
	var dbuser pb.User
	log.Println("Read db id ", userID)
	updateTime := time.Now()
	createTime := time.Now()
	if err := dbConn.QueryRow(context.Background(), "select userid,firstname,lastname,age,updatedate,createdate from cqrs_user where userid=$1", userID).Scan(&dbuser.UserId, &dbuser.FirstName, &dbuser.LastName, &dbuser.Age, &updateTime, &createTime); err != nil {
		log.Printf("QueryRow failed: %v\n", err)
		return pb.User{}
	}
	dbuser.UpdateDate = createTime.UnixNano() / 1000000
	dbuser.CreateDate = updateTime.UnixNano() / 1000000
	log.Println(dbuser)
	return dbuser
}

// updateUserInDb Update User From Database
func updateUserInDb(user pb.User) pb.User {
	log.Println("update")
	value, err := dbConn.Exec(context.Background(), "update cqrs_user set firstname = $2, lastname = $3, age = $4, updatedate = $5 where userid = $1 ", user.UserId, user.FirstName, user.LastName, user.Age, time.Now())
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(value)
	log.Println(user)
	return readUserInDb(user.UserId)
}

// deleteUserInDb Delete User From Database
func deleteUserInDb(userID int64) {
	log.Println("delete User in db ", userID)
	value, err := dbConn.Exec(context.Background(), "delete from cqrs_user where userid = $1 ", userID)
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(value)
	log.Println(userID)
}

// Consumer Functions

// createUser From Kafka Message
func createUser(kafkaMsg kafka.Message) {
	user := unmarshal(kafkaMsg)
	log.Println("Create User = ", user)
	createUserInDb(user)
	setInRedis(user)
}

// readUser From Kafka Message
func readUser(id int64) pb.User {
	log.Printf("REad Id = %d", id)
	user := getInRedis(id)
	return user
}

// updateUser From Kafka Message
func updateUser(kafkaMsg kafka.Message) {
	user := unmarshal(kafkaMsg)
	log.Println("Update User ", user)
	updateUserInDb(user)
	setInRedis(user)
}

// deleteUser From Kafka Message
func deleteUser(kafkaMsg kafka.Message) {
	userID := unmarshalID(kafkaMsg)
	log.Println("Delete User ", userID)
	deleteUserInDb(userID.Id)
	removeInRedis(userID.Id)
}

// unmarshal User Message From Kafka
func unmarshal(kafkaMsg kafka.Message) pb.User {
	var user pb.User
	if err := json.Unmarshal(kafkaMsg.Value, &user); err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return user
}

// unmarshalID UserID Message From Kafka
func unmarshalID(kafkaMsg kafka.Message) pb.Id {
	var userID pb.Id
	if err := json.Unmarshal(kafkaMsg.Value, &userID); err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return userID
}

// validate the User
func validate(user *pb.User) error {
	return nil
}
