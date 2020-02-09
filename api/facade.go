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
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	//	Make sur to use same name and port in you deployment
	conn, err := grpc.Dial("user-service:3000", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	userServiceClient := pb.NewUserServiceClient(conn)

	routes := mux.NewRouter()
	routes.HandleFunc("/", indexHandler).Methods("GET")
	//For simplify the tests we use GET but must be change
	//Create Methode: Post
	routes.HandleFunc("/api/user/create/{id}/firstname/{firstname}/lastname/{lastname}/age/{age}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		fmt.Println("Facade Create User")
		vars := mux.Vars(r)
		if err != nil {
			json.NewEncoder(w).Encode("Invalid parameter")
		}

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
		if result, err := userServiceClient.CreateUser(ctx, user); err == nil {
			msg := fmt.Sprintf("Response is %s", result.Msg)
			code := fmt.Sprintf("Response is %d", result.Code)
			json.NewEncoder(w).Encode(msg)
			json.NewEncoder(w).Encode(code)
		} else {
			errorMsg := fmt.Sprintf("Creation Internal Error: %s !!!!", err.Error())
			json.NewEncoder(w).Encode(errorMsg)
		}
	}).Methods("GET") //Create

	//Read Methode: Get
	routes.HandleFunc("/api/user/read/{id}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		fmt.Println("Facade Read User1")
		vars := mux.Vars(r)
		if err != nil {
			json.NewEncoder(w).Encode("Invalid parameter")
		}
		fmt.Println("Facade Read User2")
		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()
		fmt.Println("Facade Read User3")
		id, errid := strconv.ParseInt(vars["id"], 10, 64)
		if errid != nil {
			json.NewEncoder(w).Encode("Invalid parameter Id")
		}
		fmt.Println("Facade Read User4")
		userID := pb.Id{Id: id}
		fmt.Println(userID)
		//Send the response back
		if user, err := userServiceClient.ReadUser(ctx, &userID); err == nil {
			fmt.Println(user)
			json.NewEncoder(w).Encode(user)
		} else {
			errorMsg := fmt.Sprintf("Internal Error: %s !!!!", err.Error())
			json.NewEncoder(w).Encode(errorMsg)
		}
		fmt.Println("Facade Read User5")
	}).Methods("GET") //Read

	//Update Methode: Put
	routes.HandleFunc("/api/user/update/{id}/firstname/{firstname}/lastname/{lastname}/age/{age}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		fmt.Println("Facade Update User")
		vars := mux.Vars(r)
		if err != nil {
			json.NewEncoder(w).Encode("Invalid parameter MSG")
		}

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
			msg := fmt.Sprintf("Response is %s", result.Msg)
			code := fmt.Sprintf("Response is %d", result.Code)
			json.NewEncoder(w).Encode(msg)
			json.NewEncoder(w).Encode(code)
		} else {
			errorMsg := fmt.Sprintf("Internal Error: %s !!!!", err.Error())
			json.NewEncoder(w).Encode(errorMsg)
		}
	}).Methods("GET") //Update

	//Create Methode: Post
	routes.HandleFunc("/api/user/delete/{id}/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")
		fmt.Println("Facade Delete User")
		vars := mux.Vars(r)
		if err != nil {
			json.NewEncoder(w).Encode("Invalid parameter MSG")
		}

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		id, errid := strconv.ParseInt(vars["id"], 10, 64)
		if errid != nil {
			json.NewEncoder(w).Encode("Invalid parameter Id")
		}

		userID := pb.Id{Id: id}
		fmt.Println(id)
		//Send the response back
		if user, err := userServiceClient.DeleteUser(ctx, &userID); err == nil {
			json.NewEncoder(w).Encode(user)
		} else {
			errorMsg := fmt.Sprintf("Internal Error: %s !!!!", err.Error())
			json.NewEncoder(w).Encode(errorMsg)
		}
	}).Methods("GET") //Delete

	//Listening for Rest calls
	fmt.Println("Application is running on : 8080 .....")
	http.ListenAndServe(":8080", routes)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UFT-8")
	json.NewEncoder(w).Encode("Server is running")
}
