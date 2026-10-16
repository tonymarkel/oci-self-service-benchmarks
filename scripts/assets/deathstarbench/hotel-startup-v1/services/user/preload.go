package user

import (
	"fmt"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/startup"
	"go.mongodb.org/mongo-driver/mongo"
)

func loadUsers(client *mongo.Client) (map[string]string, error) {
	var users []User
	if startup.LoadRows(client, "user", "user", &users) != nil {
		return nil, fmt.Errorf("candidate Hotel user preload refused")
	}
	loaded := make(map[string]string, len(users))
	for _, user := range users {
		loaded[user.Username] = user.Password
	}
	return loaded, nil
}
