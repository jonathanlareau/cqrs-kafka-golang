/**
 * jonathan.lareau@gmail.com
 *
 * Very simple service facade api
 *
 * Facade for services user, proder and order
 *
 **/
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	//	Make sur to use same name and port in you deployment

	facadeport := os.Getenv("FACADE_SERVICE_PORT")

	userport := os.Getenv("USER_SERVICE_PORT")

	productport := os.Getenv("PRODUCT_SERVICE_PORT")

	orderport := os.Getenv("ORDER_SERVICE_PORT")

	orderdtoport := os.Getenv("ORDERDTO_SERVICE_PORT")

	connuser, erruser := grpc.Dial("user-service:"+userport, grpc.WithInsecure())
	if erruser != nil {
		panic(erruser)
	}
	connproduct, errproduct := grpc.Dial("product-service:"+productport, grpc.WithInsecure())
	if errproduct != nil {
		panic(errproduct)
	}
	connorder, errorder := grpc.Dial("order-service:"+orderport, grpc.WithInsecure())
	if errorder != nil {
		panic(errorder)
	}
	connorderdto, errorderdto := grpc.Dial("orderdto-service:"+orderdtoport, grpc.WithInsecure())
	if errorderdto != nil {
		panic(errorderdto)
	}
	userServiceClient := pb.NewUserServiceClient(connuser)
	productServiceClient := pb.NewProductServiceClient(connproduct)
	orderServiceClient := pb.NewOrderServiceClient(connorder)
	orderDtoServiceClient := pb.NewOrderDtoServiceClient(connorderdto)

	routes := mux.NewRouter()
	routes.HandleFunc("/", indexHandler).Methods("GET")
	//For simplify the tests we use GET but must be change
	//Create Methode: Post
	routes.HandleFunc("/api/user/create/firstname/{firstname}/lastname/{lastname}/age/{age}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		log.Println("Facade Create User")
		vars := mux.Vars(r)

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		age, errage := strconv.ParseInt(vars["age"], 10, 32)
		if errage != nil {
			json.NewEncoder(w).Encode("Invalid parameter age")
		}
		user := &pb.User{FirstName: vars["firstname"], LastName: vars["lastname"], Age: int32(age)}

		//Send the response back
		if newUser, err := userServiceClient.CreateUser(ctx, user); err == nil {
			json.NewEncoder(w).Encode(newUser)
		} else {
			json.NewEncoder(w).Encode(err.Error())
		}
	}).Methods("GET") //Create

	//Read Methode: Get
	routes.HandleFunc("/api/user/read/{id}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		log.Println("Facade Read User")
		vars := mux.Vars(r)

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		id, errid := strconv.ParseInt(vars["id"], 10, 64)
		if errid != nil {
			json.NewEncoder(w).Encode("Invalid parameter Id")
		}

		userID := pb.Id{Id: id}
		log.Println(userID)
		//Send the response back
		if user, err := userServiceClient.ReadUser(ctx, &userID); err == nil {
			json.NewEncoder(w).Encode(user)
		} else {
			json.NewEncoder(w).Encode(err.Error())
		}
	}).Methods("GET") //Read

	//Read Methode: Get
	routes.HandleFunc("/api/product/read/{id}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		log.Println("Facade Read User")
		vars := mux.Vars(r)

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		id, errid := strconv.ParseInt(vars["id"], 10, 64)
		if errid != nil {
			json.NewEncoder(w).Encode("Invalid parameter Id")
		}

		productID := pb.Id{Id: id}
		log.Println(productID)
		//Send the response back
		if user, err := productServiceClient.ReadProduct(ctx, &productID); err == nil {
			json.NewEncoder(w).Encode(user)
		} else {
			json.NewEncoder(w).Encode(err.Error())
		}
	}).Methods("GET") //Read

	//Read Methode: Get
	routes.HandleFunc("/api/order/read/{id}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		log.Println("Facade Read User")
		vars := mux.Vars(r)

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		id, errid := strconv.ParseInt(vars["id"], 10, 64)
		if errid != nil {
			json.NewEncoder(w).Encode("Invalid parameter Id")
		}

		orderID := pb.Id{Id: id}
		log.Println(orderID)
		//Send the response back
		if user, err := orderServiceClient.ReadOrder(ctx, &orderID); err == nil {
			json.NewEncoder(w).Encode(user)
		} else {
			json.NewEncoder(w).Encode(err.Error())
		}
	}).Methods("GET") //Read

	//Read Methode: Get
	routes.HandleFunc("/api/orderdto/read/{id}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		log.Println("Facade Read User")
		vars := mux.Vars(r)

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		id, errid := strconv.ParseInt(vars["id"], 10, 64)
		if errid != nil {
			json.NewEncoder(w).Encode("Invalid parameter Id")
		}

		orderID := pb.Id{Id: id}
		log.Println(orderID)
		//Send the response back
		if orderdto, err := orderDtoServiceClient.ReadOrderDto(ctx, &orderID); err == nil {
			json.NewEncoder(w).Encode(orderdto)
		} else {
			json.NewEncoder(w).Encode(err.Error())
		}
	}).Methods("GET") //Read

	//Update Methode: Put
	routes.HandleFunc("/api/user/update/{id}/firstname/{firstname}/lastname/{lastname}/age/{age}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		log.Println("Facade Update User")
		vars := mux.Vars(r)

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		id, errid := strconv.ParseInt(vars["id"], 10, 64)
		if errid != nil {
			json.NewEncoder(w).Encode("Invalid parameter Id")
		}
		age, errage := strconv.ParseInt(vars["age"], 10, 32)
		if errage != nil {
			json.NewEncoder(w).Encode("Invalid parameter age")
		}
		user := &pb.User{UserId: id, FirstName: vars["firstname"], LastName: vars["lastname"], Age: int32(age)}

		//Send the response back
		if result, err := userServiceClient.UpdateUser(ctx, user); err == nil {
			json.NewEncoder(w).Encode(result.Msg)
		} else {
			json.NewEncoder(w).Encode(err.Error())
		}
	}).Methods("GET") //Update

	//Create Methode: Post
	routes.HandleFunc("/api/user/delete/{id}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		log.Println("Facade Delete User")
		vars := mux.Vars(r)

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		id, errid := strconv.ParseInt(vars["id"], 10, 64)
		if errid != nil {
			json.NewEncoder(w).Encode("Invalid parameter Id")
		}

		userID := pb.Id{Id: id}
		log.Println(id)
		//Send the response back
		if user, err := userServiceClient.DeleteUser(ctx, &userID); err == nil {
			json.NewEncoder(w).Encode(user)
		} else {
			json.NewEncoder(w).Encode(err.Error())
		}
	}).Methods("GET") //Delete

	//Listening for Rest calls
	log.Printf("Application is running on : %s .....", facadeport)
	http.ListenAndServe(":"+facadeport, routes)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UFT-8")
	json.NewEncoder(w).Encode("Server is running")
}
