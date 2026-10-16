package geo

import (
	"fmt"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/startup"
	"github.com/hailocab/go-geoindex"
	"go.mongodb.org/mongo-driver/mongo"
)

// newGeoIndex uses the same detached rows that passed exact own-store baseline
// verification. It never performs an unchecked second MongoDB query.
func newGeoIndex(client *mongo.Client) (*geoindex.ClusteringIndex, error) {
	var points []*point
	if startup.LoadRows(client, "geo", "geo", &points) != nil {
		return nil, fmt.Errorf("candidate Hotel geo preload refused")
	}
	index := geoindex.NewClusteringIndex()
	for _, point := range points {
		index.Add(point)
	}
	return index, nil
}
