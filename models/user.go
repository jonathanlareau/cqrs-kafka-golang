
package model

import (
	pb "github.com/jonathanlareau/cqrs-kafka-golang/proto"
	"fmt"
)

// User struc to use every where
type User struct {
	UserId int64 `json:"userId"`
	FirstName string `json:"firstName"`
	LastName string `json:"lastName"`
	Age    int32 `json:"age"`
}

func (u User) String() string {
	return fmt.Sprintf("{\"userId\":\"%d\",\"firstName\":\"%s\",\"lastName\":\"%s\",\"age\":\"%d\"}", u.UserId,u.FirstName,u.LastName,u.Age)
}


// NewUser create a new user
func NewUser(userId int64, firstName, lastName string, age int32) User {
	return User{userId, firstName, lastName, age}
}

// ConvertToProto to convert user from json to proto for grcp
func ConvertToProto(user User) pb.User {
	protoUser := pb.User{UserId: user.UserId, FirstName: user.FirstName, LastName: user.LastName, Age: user.Age}
	return protoUser
}

// ConvertToJson to convert user from grpc to json for kafka
func ConvertToJson(user pb.User) User {
	jsonUser := NewUser(user.UserId, user.FirstName, user.LastName, user.Age)
	return jsonUser
}
