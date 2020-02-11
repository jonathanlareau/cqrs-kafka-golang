/**
 * jonathan.lareau@gmail.com
 *
 * CQRS Product Entity Microservice App
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
var createProductPublisher cqrs.Publisher = nil
var updateProductPublisher cqrs.Publisher = nil
var deleteProductPublisher cqrs.Publisher = nil
var createProductConsumer cqrs.Consumer = nil
var updateProductConsumer cqrs.Consumer = nil
var deleteProductConsumer cqrs.Consumer = nil
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
		brokers            = "kafka-zookeeper:9092"
		createProductTopic = "product-create"
		updateProductTopic = "product-update"
		deleteProductTopic = "product-delete"
	)

	// Create the Producers
	createProductPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), createProductTopic)
	updateProductPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), updateProductTopic)
	deleteProductPublisher = cqrs.NewPublisher(strings.Split(brokers, ","), deleteProductTopic)

	// Create the consumers
	createProductConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), createProductTopic)
	updateProductConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), updateProductTopic)
	deleteProductConsumer = cqrs.NewConsumer(strings.Split(brokers, ","), deleteProductTopic)

	// Initialize the Read in Consumers
	go func() {
		createProductConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { createProduct(kafkaMsg) })
	}()
	go func() {
		updateProductConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { updateProduct(kafkaMsg) })
	}()
	go func() {
		deleteProductConsumer.Read(context.Background(), func(kafkaMsg kafka.Message) { deleteProduct(kafkaMsg) })
	}()

	// Prepare to Receive Call from GRPC Client
	lis, err := net.Listen("tcp", ":3001")
	if err != nil {
		log.Fatalf("Failed to listen on port 3001:  %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterProductServiceServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// Functions for the Producers

// Depending of your ID's generation strategies you need to adapt this function
// Response to CreateProduct Service
func (s *server) CreateProduct(cxt context.Context, product *pb.Product) (*pb.Product, error) {
	log.Println("CreateProduct")
	if err := validate(product); err != nil {
		return product, err
	}
	newProduct := pb.Product{}
	dbproduct := createProductInDb(newProduct)
	product.ProductId = dbproduct.ProductId
	if err := updateProductPublisher.Publish(context.Background(), product); err != nil {
		log.Fatal(err)
	}
	return product, nil
}

// Response to ReadProduct Service
func (s *server) ReadProduct(cxt context.Context, id *pb.Id) (*pb.Product, error) {
	log.Println("ReadProduct ", id)
	product := readProduct(id.Id)
	return &product, nil
}

// Response to UpdateProduct Service
func (s *server) UpdateProduct(cxt context.Context, product *pb.Product) (*pb.Result, error) {
	log.Println("UpdateProduct ", product)
	if err := validate(product); err != nil {
		return &pb.Result{Code: 1, Msg: "None valid product"}, err
	}
	if err := updateProductPublisher.Publish(context.Background(), product); err != nil {
		log.Fatal(err)
	}
	return &pb.Result{Code: 0, Msg: "Update Product Message Published"}, nil
}

// Response to DeleteProduct Service
func (s *server) DeleteProduct(cxt context.Context, id *pb.Id) (*pb.Result, error) {
	log.Println("DeleteProduct", id)
	if err := deleteProductPublisher.Publish(context.Background(), id); err != nil {
		log.Fatal(err)
	}
	return &pb.Result{Code: 0, Msg: "Delete Product Message Published"}, nil
}

// Response to CreateSyncProduct Service
func (s *server) CreateSyncProduct(cxt context.Context, product *pb.Product) (*pb.Product, error) {
	log.Println("CreateSyncProduct")
	if err := validate(product); err != nil {
		return product, err
	}
	dbproduct := createProductInDb(*product)
	return &dbproduct, nil
}

// Response to ReadSyncProduct Service
func (s *server) ReadSyncProduct(cxt context.Context, id *pb.Id) (*pb.Product, error) {
	log.Println("ReadProduct ", id)
	product := readProductInDb(id.Id)
	return &product, nil
}

// Response to UpdateSyncProduct Service
func (s *server) UpdateSyncProduct(cxt context.Context, product *pb.Product) (*pb.Result, error) {
	log.Println("UpdateProduct ", product)
	if err := validate(product); err != nil {
		return &pb.Result{Code: 1, Msg: "None valid product"}, err
	}
	updateProductInDb(*product)
	return &pb.Result{Code: 0, Msg: "Update Product Sync Mode"}, nil
}

// Response to DeleteProduct Service
func (s *server) DeleteSyncProduct(cxt context.Context, id *pb.Id) (*pb.Result, error) {
	log.Println("DeleteSyncProduct", id)
	deleteProductInDb(id.Id)
	return &pb.Result{Code: 0, Msg: "Delete Product Sync Mode"}, nil
}

// Response to GetProducts Service
func (s *server) GetProducts(*pb.Product, pb.ProductService_GetProductsServer) error {
	return nil
}

//Functions For Redis

// setInRedis Set a Product Entry in Redis
func setInRedis(product pb.Product) {
	log.Println("setInRedis ", product)
	marsh, errMarsh := json.Marshal(product)
	if errMarsh != nil {
		log.Printf("Error while Unmarshal a message: %v", errMarsh)
	}
	_, errRedis := redisClient.Do("SET", "product-"+strconv.FormatInt(product.ProductId, 10), marsh)
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
}

// getInRedis Get a Product Entry in Redis
func getInRedis(id int64) pb.Product {
	log.Println("getInRedis ", id)
	newProduct, errRedis := redis.Bytes(redisClient.Do("GET", "product-"+strconv.FormatInt(id, 10)))
	if newProduct == nil {
		log.Printf("Product not found in Redis")
		product := readProductInDb(id)
		setInRedis(product)
		return product
	}
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
	var product pb.Product
	err := json.Unmarshal([]byte(newProduct), &product)
	if err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return product
}

// removeInRedis Remove a Product Entry in Redis
func removeInRedis(id int64) {
	log.Println("removeInRedis ", id)
	_, errRedis := redisClient.Do("DEL", "product-"+strconv.FormatInt(id, 10))
	if errRedis != nil {
		log.Printf("Error while Unmarshal a message: %v", errRedis)
	}
}

//Postgresql Functions

// createProductInDb Create Product In Database
func createProductInDb(product pb.Product) pb.Product {
	log.Println("Create product id DB ", product)
	var productID int64
	err := dbConn.QueryRow(context.Background(), "insert into cqrs_product (name, description, updatedate, createdate) values( $1,$2,$3,$4) RETURNING productid", product.Name, product.Description, time.Now(), time.Now()).Scan(&productID)
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(productID)
	return readProductInDb(productID)
}

// readProductInDb Read Product In Database
func readProductInDb(productID int64) pb.Product {
	var dbproduct pb.Product
	log.Println("Read db id ", productID)
	updateTime := time.Now()
	createTime := time.Now()
	if err := dbConn.QueryRow(context.Background(), "select productid,name,description,updatedate,createdate from cqrs_product where productid=$1", productID).Scan(&dbproduct.ProductId, &dbproduct.Name, &dbproduct.Description, &updateTime, &createTime); err != nil {
		log.Printf("QueryRow failed: %v\n", err)
		return pb.Product{}
	}
	dbproduct.UpdateDate = createTime.UnixNano() / 1000000
	dbproduct.CreateDate = updateTime.UnixNano() / 1000000
	log.Println(dbproduct)
	return dbproduct
}

// updateProductInDb Update Product From Database
func updateProductInDb(product pb.Product) pb.Product {
	log.Println("update")
	value, err := dbConn.Exec(context.Background(), "update cqrs_product set firstname = $2, lastname = $3, age = $4, updatedate = $5 where productid = $1 ", product.ProductId, product.Name, product.Description, time.Now())
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(value)
	log.Println(product)
	return readProductInDb(product.ProductId)
}

// deleteProductInDb Delete Product From Database
func deleteProductInDb(productID int64) {
	log.Println("delete Product in db ", productID)
	value, err := dbConn.Exec(context.Background(), "delete from cqrs_product where productid = $1 ", productID)
	if err != nil {
		log.Fatalf("QueryRow failed: %v\n", err)
	}
	log.Println(value)
	log.Println(productID)
}

// Consumer Functions

// createProduct From Kafka Message
func createProduct(kafkaMsg kafka.Message) {
	product := unmarshal(kafkaMsg)
	log.Println("Create Product = ", product)
	createProductInDb(product)
	setInRedis(product)
}

// readProduct From Kafka Message
func readProduct(id int64) pb.Product {
	log.Printf("REad Id = %d", id)
	product := getInRedis(id)
	return product
}

// updateProduct From Kafka Message
func updateProduct(kafkaMsg kafka.Message) {
	product := unmarshal(kafkaMsg)
	log.Println("Update Product ", product)
	updateProductInDb(product)
	setInRedis(product)
}

// deleteProduct From Kafka Message
func deleteProduct(kafkaMsg kafka.Message) {
	productID := unmarshalID(kafkaMsg)
	log.Println("Delete Product ", productID)
	deleteProductInDb(productID.Id)
	removeInRedis(productID.Id)
}

// unmarshal Product Message From Kafka
func unmarshal(kafkaMsg kafka.Message) pb.Product {
	var product pb.Product
	if err := json.Unmarshal(kafkaMsg.Value, &product); err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return product
}

// unmarshalID ProductID Message From Kafka
func unmarshalID(kafkaMsg kafka.Message) pb.Id {
	var productID pb.Id
	if err := json.Unmarshal(kafkaMsg.Value, &productID); err != nil {
		log.Printf("Error while Unmarshal a message: %v", err)
	}
	return productID
}

// validate the Product
func validate(product *pb.Product) error {
	return nil
}
