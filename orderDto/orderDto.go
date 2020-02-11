/**
 * jonathan.lareau@gmail.com
 *
 * CQRS OrderDto Entity Microservice App
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
	"time"

	"github.com/gomodule/redigo/redis"
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// For receive services fucntions calls
type server struct{}

// Global Variables
var redisClient redis.Conn = nil

func main() {
	var err error

	orderdtoport := os.Getenv("ORDERDTO_SERVICE_PORT")

	log.Println("username : ", os.Getenv("USERNAME"))

	// Initilize the connection with Postgresql
	redisClient, err = redis.Dial("tcp", "redis:6379")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to Redis")
	defer redisClient.Close()

	// Prepare to Receive Call from GRPC Client
	orderport := os.Getenv("ORDERDTO_SERVICE_PORT")
	log.Printf("Application is running on : %s .....", orderport)
	lis, err := net.Listen("tcp", ":" + orderdtoport)
	if err != nil {
		log.Fatalf("Failed to listen on port %s:  %v", orderdtoport, err)
	}
	s := grpc.NewServer()
	pb.RegisterOrderDtoServiceServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// Functions for the Producers

// Response to ReadOrderDto Service
func (s *server) ReadOrderDto(cxt context.Context, id *pb.Id) (*pb.OrderDto, error) {
	log.Println("ReadOrderDto ", id)
    orderDto := getInRedis(id.Id)
    if orderDto.Order.OrderId != int64(0) {
		return &orderDto, nil
	} else {
		log.Println("ReadOrderDto 4")

	userport := os.Getenv("USER_SERVICE_PORT")

	productport := os.Getenv("PRODUCT_SERVICE_PORT")

	orderport := os.Getenv("ORDER_SERVICE_PORT")

	log.Println("ReadOrderDto ", id)
	connUser, errUser := grpc.Dial("user-service:"+userport, grpc.WithInsecure())
	connProduct, errProduct := grpc.Dial("product-service:"+productport, grpc.WithInsecure())
	connOrder, errOrder := grpc.Dial("order-service:"+orderport, grpc.WithInsecure())
	if errUser != nil {
		panic(errUser)
	}
	if errProduct != nil {
		panic(errProduct)
	}
	if errOrder != nil {
		panic(errOrder)
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	defer cancel()

	orderServiceClient := pb.NewOrderServiceClient(connOrder)
	order, errReadOrder := orderServiceClient.ReadOrder(ctx, id)
	log.Println(order)
	log.Println(order.OrderId)
	log.Println(order.UserId)
	log.Println(order.UserId)
	log.Println(order.ProductId)
	log.Println(order.UpdateDate)
	if errReadOrder != nil {
		panic(errReadOrder)
	}

	userID := pb.Id{Id: order.UserId}
	userServiceClient := pb.NewUserServiceClient(connUser)
	user, errReadUser := userServiceClient.ReadUser(ctx, &userID)
	log.Println(user)
	log.Println(user.UserId)
	log.Println(user.FirstName)
	log.Println(user.LastName)
	if errReadUser != nil {
		panic(errReadUser)
	}

	productID := pb.Id{Id: order.ProductId}
	productServiceClient := pb.NewProductServiceClient(connProduct)
	product, errReadProduct := productServiceClient.ReadProduct(ctx, &productID)
	log.Println(product)
	if errReadProduct != nil {
		panic(errReadProduct)
	}

	orderDto := &pb.OrderDto{Order: order, User: user, Product: product}

	setInRedis(*orderDto)

	return orderDto, nil
			
}

}

//Functions For Redis

// setInRedis Set a OrderDto Entry in Redis
func setInRedis(orderDto pb.OrderDto) {
	log.Println("setInRedis ")
	marsh, errMarsh := json.Marshal(orderDto)
	if errMarsh != nil {
		log.Printf("Error while Unmarshal a message: %v", errMarsh)
	}
	_, errRedis := redisClient.Do("SET", "orderDto-"+strconv.FormatInt(orderDto.Order.OrderId, 10), marsh)
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
}

// getInRedis Get a OrderDto Entry in Redis
func getInRedis(id int64) pb.OrderDto {
	log.Println("getInRedis ", id)
	newOrderDto, errRedis := redis.Bytes(redisClient.Do("GET", "orderDto-"+strconv.FormatInt(id, 10)))

	if len(newOrderDto) == 0 {
		orderDto := pb.OrderDto{}
        orderDto.Order = &pb.Order{OrderId:0}
		return orderDto
	}
	
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}

	var orderDto pb.OrderDto
	err := json.Unmarshal([]byte(newOrderDto), &orderDto)
	if err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return orderDto
}

// removeInRedis Remove a OrderDto Entry in Redis
func removeInRedis(id int64) {
	log.Println("removeInRedis ", id)
	_, errRedis := redisClient.Do("DEL", "orderDto-"+strconv.FormatInt(id, 10))
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
}
