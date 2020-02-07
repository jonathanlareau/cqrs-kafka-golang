/**
 * jonathan.lareau@gmail.com
 *
 * Very simple service facade api
 *
 **/

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	count := 0

	//	Make sur to use same name and port in you deployment
	//conn, err := grpc.Dial("sayhello-service:3000", grpc.WithInsecure())
	conn, err := grpc.Dial("sayhello-service:3000", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	userServiceClient := pb.NewUserServiceClient(conn)

	routes := mux.NewRouter()
	routes.HandleFunc("/", indexHandler).Methods("GET")
	routes.HandleFunc("/api/sayhello/{msg}", func(w http.ResponseWriter, r *http.Request) {
		count++
		w.Header().Set("Content-Type", "application/json; charset=UFT-8")

		vars := mux.Vars(r)
		if err != nil {
			json.NewEncoder(w).Encode("Invalid parameter MSG")
		}

		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		user := &pb.User{UserId: int64(count), FirstName: vars["msg"], LastName: "Lareau", Age: 44}

		//Call server in grpc
		//req := &pb.SayHiRequest{RequestMsg: vars["msg"]}

		//Send the response back
		if result, err := userServiceClient.CreateUser(ctx, user); err == nil {
			msg := fmt.Sprintf("Response is %s", result.Msg)
			json.NewEncoder(w).Encode(msg)
		} else {
			errorMsg := fmt.Sprintf("Internal Error: %s !!!!", err.Error())
			json.NewEncoder(w).Encode(errorMsg)
		}
	}).Methods("GET")

	//Listening for Rest calls
	fmt.Println("Application is running on : 8080 .....")
	http.ListenAndServe(":8080", routes)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UFT-8")
	json.NewEncoder(w).Encode("Server is running")
}
