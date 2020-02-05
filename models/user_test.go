
package model

import (
	"testing"
	"fmt"
)


// NewUser create a new user
func TestNewUser(t *testing.T) {
	user := User{0, "Jonathan", "Lareau", 44}
	if user.Age != 44 {
		t.Errorf("Age was incorrect, got: %d, want: %d.", user.Age, 44)
	}
}

func TestConvert(t *testing.T) {
	user := User{0, "Jonathan", "Lareau", 44}
	protoUser := ConvertToProto(user)
	jsonUser := ConvertToJson(protoUser)
	fmt.Println("user     = " + user.String())
	fmt.Println("jsonUser = " + jsonUser.String())
	if user != jsonUser {
		t.Errorf("User Convertion Fail")
	}
	protoUser.Age = 43
	jsonUser = ConvertToJson(protoUser)
	fmt.Println("user     = " + user.String())
	fmt.Println("jsonUser = " + jsonUser.String())
	if user == jsonUser {
		t.Errorf("User Convertion Fail")
	}
}