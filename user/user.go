package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"os"

	"github.com/gomodule/redigo/redis"
	"github.com/jonathanlareau/cqrs-kafka-golang/cqrs"
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"github.com/jackc/pgx/v4"
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
var dbConn *pgx.Conn

func main() {
	var err error
	redisClient, err = redis.Dial("tcp", "192.168.99.101:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer redisClient.Close()

	//conn = pg.DB(host="localhost", user="USERNAME", passwd="PASSWORD", dbname="DBNAME")

//db.connect(host='postgres://candidate.suade.org/company', database='randomname', user='candidate', password='abc', port='5432')

	//conn, err := pgx.Connect(context.Background(), "host="192.168.99.101,port='5432'")
	//var err error
	dbConn, err = pgx.Connect(context.Background(), "postgresql://postgres:password@192.168.99.101:5432/cqrs")
	fmt.Println(dbConn)
	//conn, err := pgx.Connect(context.Background(), "host='192.168.99.101', database='cqrs_user',user='postgres', password='password',port='5432'")
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
	//	os.Exit(1)
	//}
	//defer dbConn.Close(context.Background())

	var userid int64
	var firstname string
	var lastname string
	var age int32
	err = dbConn.QueryRow(context.Background(), "select userid,firstname,lastname,age from cqrs_user where userid=$1", 1).Scan(&userid,&firstname, &lastname,&age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(userid,firstname, lastname,age)

	var dbuser pb.User
	err = dbConn.QueryRow(context.Background(), "select userid,firstname,lastname,age from cqrs_user where userid=$1", 0).Scan(&dbuser.UserId,&dbuser.FirstName, &dbuser.LastName,&dbuser.Age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(dbuser)

	//var dbuser pb.User
	dbConn.Exec(context.Background(), "insert into cqrs_user (userid, firstname, lastname, age) values( $1,$2,$3,$4) ", 5,dbuser.FirstName,dbuser.LastName,dbuser.Age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(dbuser)

//	userf := ReadUserFromDB(5)
//	fmt.Println(userf)

    fmt.Println("next user id")
	id := ReadNextUserId()
	fmt.Println(id)

	usere := pb.User{UserId: 11, FirstName: "Jonathan", LastName: "Lareau", Age: 44}

	userf := CreateUserInDb(usere)
	fmt.Println(userf)


	usere = pb.User{UserId: 11, FirstName: "Jonathan", LastName: "Lareau", Age: 44}

	usere.FirstName="testname"
	userg := UpdateUserInDb(usere)
	fmt.Println(userg)

	DeleteUserFromDb(0) 

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

// Not Prod Ready... just for test
func ReadNextUserId() int64{
	var userId int64
	err := dbConn.QueryRow(context.Background(), "select max(userid) from cqrs_user").Scan(&userId)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(userId)
	userId++
	return userId
}

func ReadUserFromDb(user int64) pb.User{
	var dbuser pb.User
	//fmt.Println(dbConn)
	fmt.Println(context.Background())
	err := dbConn.QueryRow(context.Background(), "select userid,firstname,lastname,age from cqrs_user where userid=$1", 0).Scan(&dbuser.UserId,&dbuser.FirstName,&dbuser.LastName,&dbuser.Age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(dbuser)
	return dbuser
}

func CreateUserInDb(user pb.User) pb.User{
	fmt.Println("create")
	userId := ReadNextUserId()
	value, err := dbConn.Exec(context.Background(), "insert into cqrs_user (userid, firstname, lastname, age) values( $1,$2,$3,$4) ", userId, user.FirstName,  user.LastName, user.Age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(value)
	fmt.Println(userId)
	return ReadUserFromDb(userId)
}

func UpdateUserInDb(user pb.User) pb.User{
	fmt.Println("update")
	value, err := dbConn.Exec(context.Background(), "update cqrs_user set firstname = $2, lastname = $3, age = $4 where userid = $1 ", user.UserId, user.FirstName,  user.LastName, user.Age)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(value)
	fmt.Println(user)
	return ReadUserFromDb(user.UserId)
}

func DeleteUserFromDb(user int64) {
	fmt.Println("update")
	value, err := dbConn.Exec(context.Background(), "delete from cqrs_user where userid = $1 ", user)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(value)

}