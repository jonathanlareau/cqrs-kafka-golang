/**
 * jonathan.lareau@gmail.com
 *
 * CQRS Order Entity Microservice App
 *
 * Response to Facade Calls
 *
 **/
package main

import (
	"encoding/json"
	"log"
	"net"
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
var createOrderPublisher cqrs.Publisher = nil
var updateOrderPublisher cqrs.Publisher = nil
var deleteOrderPublisher cqrs.Publisher = nil
var createOrderConsumer cqrs.Consumer = nil
var updateOrderConsumer cqrs.Consumer = nil
var deleteOrderConsumer cqrs.Consumer = nil
var redisClient redis.Conn = nil
var dbConn *pgx.Conn

func main() {
	var err error

	// Initilize the connection with Postgresql
	redisClient, err = redis.Dial("tcp", "192.168.99.100:6379")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to Redis")
	defer redisClient.Close()

	// Initilize the connection with Postgresql
	dbConn, err = pgx.Connect(context.Background(), "postgresql://postgres:password@192.168.99.100:5432/cqrs")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to Postgresql")
	defer dbConn.Close(context.Background())

	// Const needed in Main Function
	const (
		brokers          = "kafka-zookeeper:9092"
		createOrderTopic = "order-create"
		updateOrderTopic = "order-update"
		deleteOrderTopic = "order-delete"
	)

	// Create the Producers
	createOrderPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), createOrderTopic)
	updateOrderPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), updateOrderTopic)
	deleteOrderPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), deleteOrderTopic)

	// Create the consumers
	createOrderConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), createOrderTopic)
	updateOrderConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), updateOrderTopic)
	deleteOrderConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), deleteOrderTopic)

	// Initialize the Read in Consumers
	go func() {
		createOrderConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { createOrder(kafkaMsg) })
	}()
	go func() {
		updateOrderConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { updateOrder(kafkaMsg) })
	}()
	go func() {
		deleteOrderConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { deleteOrder(kafkaMsg) })
	}()

	// Prepare to Receive Call from GRPC Client
	lis, err := net.Listen("tcp", ":3002")
	if err != nil {
		log.Fatalf("Failed to listen on port 3002:  %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterOrderServiceServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// Functions for the Producers

// Depending of your ID's generation strategies you need to adapt this function
// Response to CreateOrder Service
func (s *server) CreateOrder(cxt context.Context, order *pb.Order) (*pb.Order, error) {
	log.Println("CreateOrder")
	if err := validate(order); err != nil {
		return order, err
	}
	newOrder := pb.Order{}
	dborder := createOrderInDb(newOrder)
	order.OrderId = dborder.OrderId
	if err := updateOrderPublisher.Publish(context.Background(), order); err != nil {
		log.Fatal(err)
	}
	return order, nil
}

// Response to ReadOrder Service
func (s *server) ReadOrder(cxt context.Context, id *pb.Id) (*pb.Order, error) {
	log.Println("ReadOrder ", id)
	order := readOrder(id.Id)
	return &order, nil
}

// Response to UpdateOrder Service
func (s *server) UpdateOrder(cxt context.Context, order *pb.Order) (*pb.Result, error) {
	log.Println("UpdateOrder ", order)
	if err := validate(order); err != nil {
		return &pb.Result{Code: 1, Msg: "None valid order"}, err
	}
	if err := updateOrderPublisher.Publish(context.Background(), order); err != nil {
		log.Fatal(err)
	}
	return &pb.Result{Code: 0, Msg: "Update Order Message Published"}, nil
}

// Response to DeleteOrder Service
func (s *server) DeleteOrder(cxt context.Context, id *pb.Id) (*pb.Result, error) {
	log.Println("DeleteOrder", id)
	if err := deleteOrderPublisher.Publish(context.Background(), id); err != nil {
		log.Fatal(err)
	}
	return &pb.Result{Code: 0, Msg: "Delete Order Message Published"}, nil
}

// Response to CreateSyncOrder Service
func (s *server) CreateSyncOrder(cxt context.Context, order *pb.Order) (*pb.Order, error) {
	log.Println("CreateSyncOrder")
	if err := validate(order); err != nil {
		return order, err
	}
	dborder := createOrderInDb(*order)
	return &dborder, nil
}

// Response to ReadSyncOrder Service
func (s *server) ReadSyncOrder(cxt context.Context, id *pb.Id) (*pb.Order, error) {
	log.Println("ReadOrder ", id)
	order := readOrderInDb(id.Id)
	return &order, nil
}

// Response to UpdateSyncOrder Service
func (s *server) UpdateSyncOrder(cxt context.Context, order *pb.Order) (*pb.Result, error) {
	log.Println("UpdateOrder ", order)
	if err := validate(order); err != nil {
		return &pb.Result{Code: 1, Msg: "None valid order"}, err
	}
	updateOrderInDb(*order)
	return &pb.Result{Code: 0, Msg: "Update Order Sync Mode"}, nil
}

// Response to DeleteOrder Service
func (s *server) DeleteSyncOrder(cxt context.Context, id *pb.Id) (*pb.Result, error) {
	log.Println("DeleteSyncOrder", id)
	deleteOrderInDb(id.Id)
	return &pb.Result{Code: 0, Msg: "Delete Order Sync Mode"}, nil
}

// Response to GetOrders Service
func (s *server) GetOrders(*pb.Order, pb.OrderService_GetOrdersServer) error {
	return nil
}

//Functions For Redis

// setInRedis Set a Order Entry in Redis
func setInRedis(order pb.Order) {
	log.Println("setInRedis ", order)
	marsh, errMarsh := json.Marshal(order)
	if errMarsh != nil {
		log.Printf("Error while Unmarshal a message: %v", errMarsh)
	}
	_, errRedis := redisClient.Do("SET", "order-"+strconv.FormatInt(order.OrderId, 10), marsh)
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
}

// getInRedis Get a Order Entry in Redis
func getInRedis(id int64) pb.Order {
	log.Println("getInRedis ", id)
	newOrder, errRedis := redis.Bytes(redisClient.Do("GET", "order-"+strconv.FormatInt(id, 10)))
	if newOrder == nil {
		log.Printf("Order not found in Redis")
		order := readOrderInDb(id)
		setInRedis(order)
		return order
	}
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
	var order pb.Order
	err := json.Unmarshal([]byte(newOrder), &order)
	if err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return order
}

// removeInRedis Remove a Order Entry in Redis
func removeInRedis(id int64) {
	log.Println("removeInRedis ", id)
	_, errRedis := redisClient.Do("DEL", "order-"+strconv.FormatInt(id, 10))
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
}

//Postgresql Functions

// createOrderInDb Create Order In Database
func createOrderInDb(order pb.Order) pb.Order {
	log.Println("Create order id DB ", order)
	var orderID int64
	err := dbConn.QueryRow(context.Background(), "insert into cqrs_order (userid, productid, orderdate, shipdate, updatedate, createdate) values( $1,$2,$3,$4,$5,$6) RETURNING orderid", order.UserId, order.ProductId, time.Now(), time.Now(), time.Now(), time.Now()).Scan(&orderID)
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(orderID)
	return readOrderInDb(orderID)
}

// readOrderInDb Read Order In Database
func readOrderInDb(orderID int64) pb.Order {
	var dborder pb.Order
	log.Println("Read db id ", orderID)
	orderTime := time.Now()
	shipTime := time.Now()
	updateTime := time.Now()
	createTime := time.Now()
	if err := dbConn.QueryRow(context.Background(), "select orderid,firstname,lastname,age,updatedate,createdate from cqrs_order where orderid=$1", orderID).Scan(&dborder.OrderId, &dborder.UserId, &dborder.ProductId, &orderTime, &shipTime, &updateTime, &createTime); err != nil {
		log.Printf("QueryRow failed: %v\n", err)
		return pb.Order{}
	}
	dborder.OrderDate = orderTime.UnixNano() / 1000000
	dborder.ShipDate = shipTime.UnixNano() / 1000000
	dborder.UpdateDate = createTime.UnixNano() / 1000000
	dborder.CreateDate = updateTime.UnixNano() / 1000000
	log.Println(dborder)
	return dborder
}

// updateOrderInDb Update Order From Database
func updateOrderInDb(order pb.Order) pb.Order {
	log.Println("update")
	value, err := dbConn.Exec(context.Background(), "update cqrs_order set userid = $2, productid = $3, updatedate = $4 where orderid = $1 ", order.OrderId, order.UserId, order.ProductId, time.Now())
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(value)
	log.Println(order)
	return readOrderInDb(order.OrderId)
}

// deleteOrderInDb Delete Order From Database
func deleteOrderInDb(orderID int64) {
	log.Println("delete Order in db ", orderID)
	value, err := dbConn.Exec(context.Background(), "delete from cqrs_order where orderid = $1 ", orderID)
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(value)
	log.Println(orderID)
}

// Consumer Functions

// createOrder From Kafka Message
func createOrder(kafkaMsg kafka.Message) {
	order := unmarshal(kafkaMsg)
	log.Println("Create Order = ", order)
	createOrderInDb(order)
	setInRedis(order)
}

// readOrder From Kafka Message
func readOrder(id int64) pb.Order {
	log.Printf("REad Id = %d", id)
	order := getInRedis(id)
	return order
}

// updateOrder From Kafka Message
func updateOrder(kafkaMsg kafka.Message) {
	order := unmarshal(kafkaMsg)
	log.Println("Update Order ", order)
	updateOrderInDb(order)
	setInRedis(order)
}

// deleteOrder From Kafka Message
func deleteOrder(kafkaMsg kafka.Message) {
	orderID := unmarshalID(kafkaMsg)
	log.Println("Delete Order ", orderID)
	deleteOrderInDb(orderID.Id)
	removeInRedis(orderID.Id)
}

// unmarshal Order Message From Kafka
func unmarshal(kafkaMsg kafka.Message) pb.Order {
	var order pb.Order
	if err := json.Unmarshal(kafkaMsg.Value, &order); err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return order
}

// unmarshalID OrderID Message From Kafka
func unmarshalID(kafkaMsg kafka.Message) pb.Id {
	var orderID pb.Id
	if err := json.Unmarshal(kafkaMsg.Value, &orderID); err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return orderID
}

// validate the Order
func validate(order *pb.Order) error {
	return nil
}
