package reservation

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	"go.mongodb.org/mongo-driver/bson"
)

type fixtureCache struct {
	mu      sync.Mutex
	items   map[string]*memcache.Item
	nilMiss bool
	err     error
}

func (c *fixtureCache) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	result := map[string]*memcache.Item{}
	for _, key := range keys {
		if item, ok := c.items[key]; ok {
			result[key] = item
		}
	}
	if c.nilMiss && len(result) == 0 {
		return nil, nil
	}
	return result, nil // Actual gomemcache contract: absent keys and nil error.
}

func (c *fixtureCache) Set(item *memcache.Item) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[item.Key] = item
	return nil
}

type fixtureStore struct {
	mu               sync.Mutex
	capacities       map[string]int
	counts           map[string]int
	capacityRequests [][]string
	countRequests    []string
	err              error
}

func (s *fixtureStore) Capacities(_ context.Context, ids []string) (map[string]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capacityRequests = append(s.capacityRequests, append([]string{}, ids...))
	result := map[string]int{}
	for _, id := range ids {
		if cap, ok := s.capacities[id]; ok {
			result[id] = cap
		}
	}
	return result, s.err
}

func (s *fixtureStore) Count(_ context.Context, id, start, end string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := reservationCacheKey(id, start, end)
	s.countRequests = append(s.countRequests, key)
	return s.counts[key], s.err
}

func cacheItem(key string, value int) *memcache.Item {
	return &memcache.Item{Key: key, Value: []byte(strconv.Itoa(value))}
}

func TestCandidateAvailabilityColdWarmAndPartial(t *testing.T) {
	first := "1_2015-04-09_2015-04-10"
	second := "1_2015-04-10_2015-04-11"
	third := "2_2015-04-09_2015-04-10"
	fourth := "2_2015-04-10_2015-04-11"
	for _, tc := range []struct {
		name           string
		cached         map[string]*memcache.Item
		nilMiss        bool
		capacityMisses []string
		countMisses    []string
	}{
		{"cold-empty", map[string]*memcache.Item{}, false, []string{"1", "2"}, []string{first, second, third, fourth}},
		{"cold-nil", map[string]*memcache.Item{}, true, []string{"1", "2"}, []string{first, second, third, fourth}},
		{"warm", map[string]*memcache.Item{"1_cap": cacheItem("1_cap", 3), "2_cap": cacheItem("2_cap", 1), first: cacheItem(first, 1), second: cacheItem(second, 3), third: cacheItem(third, 0), fourth: cacheItem(fourth, 0)}, false, nil, nil},
		{"partial", map[string]*memcache.Item{"1_cap": cacheItem("1_cap", 3), first: cacheItem(first, 1), third: cacheItem(third, 0)}, false, []string{"2"}, []string{second, fourth}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := &fixtureCache{items: tc.cached, nilMiss: tc.nilMiss}
			store := &fixtureStore{capacities: map[string]int{"1": 3, "2": 1}, counts: map[string]int{first: 1, second: 3, third: 0, fourth: 0}}
			req := &pb.Request{HotelId: []string{"1", "2"}, InDate: "2015-04-09", OutDate: "2015-04-11", RoomNumber: 1}
			result, err := checkAvailability(context.Background(), req, cache, store)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.HotelId, []string{"2"}) {
				t.Fatalf("capacity exhaustion ignored: %v", result.HotelId)
			}
			if tc.capacityMisses == nil {
				if len(store.capacityRequests) != 0 {
					t.Fatalf("warm capacity queried database: %v", store.capacityRequests)
				}
			} else if !reflect.DeepEqual(store.capacityRequests, [][]string{tc.capacityMisses}) {
				t.Fatalf("capacity misses = %v", store.capacityRequests)
			}
			sort.Strings(store.countRequests)
			sort.Strings(tc.countMisses)
			if len(store.countRequests) != len(tc.countMisses) || (len(tc.countMisses) > 0 && !reflect.DeepEqual(store.countRequests, tc.countMisses)) {
				t.Fatalf("night misses = %v, want %v", store.countRequests, tc.countMisses)
			}
			if _, wrong := cache.items["1_2015-04-10_2015-04-10"]; wrong {
				t.Fatal("wrong date key introduced")
			}
			// A second check is genuinely warm and must not read MongoDB.
			store.capacityRequests, store.countRequests = nil, nil
			_, err = checkAvailability(context.Background(), req, cache, store)
			if err != nil || len(store.capacityRequests) != 0 || len(store.countRequests) != 0 {
				t.Fatalf("cache fill failed: %v %v %v", err, store.capacityRequests, store.countRequests)
			}
		})
	}
}

func TestCandidateCapacityQueryWrapsHotelID(t *testing.T) {
	encoded, err := bson.MarshalExtJSON(capacityMissFilter([]string{"1", "2"}), false, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"hotelId":{"$in":["1","2"]}}` {
		t.Fatalf("incorrect Mongo query: %s", encoded)
	}
}

func TestCandidateAvailabilityFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name               string
		req                *pb.Request
		cacheErr, storeErr error
		missingCapacity    bool
		corrupt            bool
	}{
		{name: "cache-error", cacheErr: errors.New("offline")},
		{name: "db-error", storeErr: errors.New("offline")},
		{name: "missing-capacity", missingCapacity: true},
		{name: "bad-count", corrupt: true},
		{name: "backwards-date", req: &pb.Request{HotelId: []string{"1"}, InDate: "2015-04-10", OutDate: "2015-04-09", RoomNumber: 1}},
		{name: "invalid-date", req: &pb.Request{HotelId: []string{"1"}, InDate: "bad", OutDate: "2015-04-10", RoomNumber: 1}},
		{name: "zero-rooms", req: &pb.Request{HotelId: []string{"1"}, InDate: "2015-04-09", OutDate: "2015-04-10", RoomNumber: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.req
			if req == nil {
				req = &pb.Request{HotelId: []string{"1"}, InDate: "2015-04-09", OutDate: "2015-04-10", RoomNumber: 1}
			}
			cache := &fixtureCache{items: map[string]*memcache.Item{}, err: tc.cacheErr}
			store := &fixtureStore{capacities: map[string]int{"1": 3}, counts: map[string]int{}, err: tc.storeErr}
			if tc.missingCapacity {
				store.capacities = map[string]int{}
			}
			if tc.corrupt {
				cache.items["1_cap"] = &memcache.Item{Key: "1_cap", Value: []byte("not-a-number")}
			}
			result, err := checkAvailability(context.Background(), req, cache, store)
			if err == nil || result != nil {
				t.Fatalf("invalid input/state admitted: %v %v", result, err)
			}
		})
	}
}

func TestCandidateNightKeys(t *testing.T) {
	nights, err := availabilityNights([]string{"1"}, "2015-04-30", "2015-05-02")
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, night := range nights {
		got = append(got, night.key)
	}
	if !reflect.DeepEqual(got, []string{"1_2015-04-30_2015-05-01", "1_2015-05-01_2015-05-02"}) {
		t.Fatalf("invalid nightly date rollover: %v", got)
	}
}
