/**
 * jonathan.lareau@gmail.com
 *
 * Very simple client example
 *
 * Receive rest call
 * Call the server in grpc
 * Return response in json
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
	 //	Make sur to use same name and port in you deployment
	 //conn, err := grpc.Dial("sayhello-service:3000", grpc.WithInsecure())
	 conn, err := grpc.Dial("sayhello-service:3000", grpc.WithInsecure())
	 if err != nil {
		 panic(err)
	 }
	 sayHelloClient := pb.NewSayHelloServiceClient(conn)
 
	 routes := mux.NewRouter()
	 routes.HandleFunc("/", indexHandler).Methods("GET")
	 routes.HandleFunc("/api/sayhello/{msg}", func(w http.ResponseWriter, r *http.Request) {
		 w.Header().Set("Content-Type", "application/json; charset=UFT-8")
 
		 vars := mux.Vars(r)
		 if err != nil {
			 json.NewEncoder(w).Encode("Invalid parameter MSG")
		 }
 
		 ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		 defer cancel()
 
		 //Call server in grpc
		 req := &pb.SayHiRequest{RequestMsg: vars["msg"]}
 
		 //Send the response back
		 if resp, err := sayHelloClient.Compute(ctx, req); err == nil {
			 msg := fmt.Sprintf("Response is %s", resp.ResponseMsg)
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