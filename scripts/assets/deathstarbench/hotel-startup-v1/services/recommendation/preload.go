package recommendation

import (
	"fmt"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/startup"
	"go.mongodb.org/mongo-driver/mongo"
)

func loadRecommendations(client *mongo.Client) (map[string]Hotel, error) {
	var hotels []Hotel
	if startup.LoadRows(client, "recommendation", "recommendation", &hotels) != nil {
		return nil, fmt.Errorf("candidate Hotel recommendation preload refused")
	}
	loaded := make(map[string]Hotel, len(hotels))
	for _, hotel := range hotels {
		loaded[hotel.HId] = hotel
	}
	return loaded, nil
}
