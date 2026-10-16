package attractions

import (
	"fmt"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/startup"
	"github.com/hailocab/go-geoindex"
	"go.mongodb.org/mongo-driver/mongo"
)

func newGeoIndex(client *mongo.Client) (*geoindex.ClusteringIndex, error) {
	var points []*point
	if startup.LoadRows(client, "attractions", "hotels", &points) != nil {
		return nil, fmt.Errorf("candidate Hotel attractions preload refused")
	}
	index := geoindex.NewClusteringIndex()
	for _, point := range points {
		index.Add(point)
	}
	return index, nil
}

func newGeoIndexRest(client *mongo.Client) (*geoindex.ClusteringIndex, error) {
	var points []*Restaurant
	if startup.LoadRows(client, "attractions", "restaurants", &points) != nil {
		return nil, fmt.Errorf("candidate Hotel attractions preload refused")
	}
	index := geoindex.NewClusteringIndex()
	for _, point := range points {
		index.Add(point)
	}
	return index, nil
}

func newGeoIndexMus(client *mongo.Client) (*geoindex.ClusteringIndex, error) {
	var points []*Museum
	if startup.LoadRows(client, "attractions", "museums", &points) != nil {
		return nil, fmt.Errorf("candidate Hotel attractions preload refused")
	}
	index := geoindex.NewClusteringIndex()
	for _, point := range points {
		index.Add(point)
	}
	return index, nil
}

func newGeoIndexCinema(client *mongo.Client) (*geoindex.ClusteringIndex, error) {
	var points []*Cinema
	// The exact baseline has no cinemas collection. LoadRows first verifies all
	// three existing attractions collections, then supplies verified-empty rows;
	// it does not create an empty collection or manufacture cinema seed data.
	if startup.LoadRows(client, "attractions", "cinemas", &points) != nil {
		return nil, fmt.Errorf("candidate Hotel attractions preload refused")
	}
	index := geoindex.NewClusteringIndex()
	for _, point := range points {
		index.Add(point)
	}
	return index, nil
}
