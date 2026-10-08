// Candidate-only correction to the audited upstream CheckAvailability method.
// Startup seeds and concurrent MakeReservation writes remain release blockers.
type reservationCache interface {
	GetMulti([]string) (map[string]*memcache.Item, error)
	Set(*memcache.Item) error
}

type availabilityStore interface {
	Capacities(context.Context, []string) (map[string]int, error)
	Count(context.Context, string, string, string) (int, error)
}

type mongoAvailabilityStore struct{ client *mongo.Client }

func capacityMissFilter(ids []string) bson.D {
	return bson.D{{"hotelId", bson.D{{"$in", ids}}}}
}

func (m mongoAvailabilityStore) Capacities(ctx context.Context, ids []string) (map[string]int, error) {
	collection := m.client.Database("reservation-db").Collection("number")
	cursor, err := collection.Find(ctx, capacityMissFilter(ids))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var numbers []number
	if err := cursor.All(ctx, &numbers); err != nil {
		return nil, err
	}
	result := make(map[string]int, len(numbers))
	for _, n := range numbers {
		if _, duplicate := result[n.HotelId]; duplicate || n.Number < 0 {
			return nil, fmt.Errorf("invalid or duplicate capacity for hotel %s", n.HotelId)
		}
		result[n.HotelId] = n.Number
	}
	return result, nil
}

func (m mongoAvailabilityStore) Count(ctx context.Context, id, start, end string) (int, error) {
	collection := m.client.Database("reservation-db").Collection("reservation")
	filter := bson.D{{"hotelId", id}, {"inDate", start}, {"outDate", end}}
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)
	var rows []reservation
	if err := cursor.All(ctx, &rows); err != nil {
		return 0, err
	}
	count := 0
	for _, row := range rows {
		if row.Number < 0 {
			return 0, fmt.Errorf("negative reserved room count for hotel %s", id)
		}
		count += row.Number
	}
	return count, nil
}

type availabilityNight struct{ id, start, end, key string }

// Shared by read and write paths: one cache key represents one hotel-night.
func reservationCacheKey(id, start, end string) string {
	return id + "_" + start + "_" + end
}

func availabilityNights(ids []string, start, end string) ([]availabilityNight, error) {
	in, err := time.Parse("2006-01-02", start)
	if err != nil {
		return nil, fmt.Errorf("invalid arrival date: %w", err)
	}
	out, err := time.Parse("2006-01-02", end)
	if err != nil || !in.Before(out) {
		return nil, fmt.Errorf("departure must be a valid date after arrival")
	}
	nights := []availabilityNight{}
	for _, id := range ids {
		for date := in; date.Before(out); date = date.AddDate(0, 0, 1) {
			next := date.AddDate(0, 0, 1)
			first, last := date.Format("2006-01-02"), next.Format("2006-01-02")
			nights = append(nights, availabilityNight{id, first, last, reservationCacheKey(id, first, last)})
		}
	}
	return nights, nil
}

func nonNegativeCacheCount(item *memcache.Item) (int, error) {
	if item == nil {
		return 0, fmt.Errorf("nil cached room count")
	}
	count, err := strconv.Atoi(string(item.Value))
	if err != nil || count < 0 {
		return 0, fmt.Errorf("invalid cached room count for %s", item.Key)
	}
	return count, nil
}

// CheckAvailability checks each night's capacity using cache hits and explicit
// requested-key differences. gomemcache.GetMulti returns absent keys with nil
// error, not ErrCacheMiss. The core is separately injectable for semantic tests.
func (s *Server) CheckAvailability(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	return checkAvailability(ctx, req, s.MemcClient, mongoAvailabilityStore{s.MongoClient})
}

func checkAvailability(ctx context.Context, req *pb.Request, cache reservationCache, store availabilityStore) (*pb.Result, error) {
	result := &pb.Result{HotelId: []string{}}
	if req == nil || req.RoomNumber <= 0 {
		return nil, fmt.Errorf("positive requested room count is required")
	}
	ids, seen := []string{}, map[string]bool{}
	for _, id := range req.HotelId {
		if id == "" || strings.Contains(id, "_") {
			return nil, fmt.Errorf("invalid hotel ID")
		}
		if !seen[id] {
			ids, seen[id] = append(ids, id), true
		}
	}
	nights, err := availabilityNights(ids, req.InDate, req.OutDate)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return result, nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = id + "_cap"
	}
	span, _ := opentracing.StartSpanFromContext(ctx, "memcached_capacity_get_multi_number")
	span.SetTag("span.kind", "client")
	items, err := cache.GetMulti(keys)
	span.Finish()
	if err != nil {
		return nil, fmt.Errorf("capacity cache read: %w", err)
	}
	capacities, misses := map[string]int{}, []string{}
	for _, id := range ids {
		if item, ok := items[id+"_cap"]; ok {
			capacity, err := nonNegativeCacheCount(item)
			if err != nil {
				return nil, err
			}
			capacities[id] = capacity
		} else {
			misses = append(misses, id)
		}
	}
	if len(misses) > 0 {
		span, _ := opentracing.StartSpanFromContext(ctx, "mongodb_capacity_get_multi_number")
		span.SetTag("span.kind", "client")
		fetched, err := store.Capacities(ctx, misses)
		span.Finish()
		if err != nil {
			return nil, fmt.Errorf("capacity database read: %w", err)
		}
		for _, id := range misses {
			capacity, ok := fetched[id]
			if !ok || capacity < 0 {
				return nil, fmt.Errorf("missing or invalid capacity for hotel %s", id)
			}
			capacities[id] = capacity
			// A failed cache fill is not a failed database read.
			cache.Set(&memcache.Item{Key: id + "_cap", Value: []byte(strconv.Itoa(capacity))})
		}
	}
	keys = make([]string, len(nights))
	for i, night := range nights {
		keys[i] = night.key
	}
	span, _ = opentracing.StartSpanFromContext(ctx, "memcached_reserve_get_multi_number")
	span.SetTag("span.kind", "client")
	items, err = cache.GetMulti(keys)
	span.Finish()
	if err != nil {
		return nil, fmt.Errorf("reservation cache read: %w", err)
	}
	type checkedNight struct {
		id    string
		count int
		err   error
	}
	// Preserve upstream concurrent database reads, but close the result channel
	// only once all explicit misses have completed. Cache hits never close it.
	checked := make(chan checkedNight, len(nights))
	var wg sync.WaitGroup
	for _, night := range nights {
		if item, hit := items[night.key]; hit {
			count, err := nonNegativeCacheCount(item)
			checked <- checkedNight{night.id, count, err}
			continue
		}
		wg.Add(1)
		go func(night availabilityNight) {
			defer wg.Done()
			span, _ := opentracing.StartSpanFromContext(ctx, "mongodb_capacity_get_multi_number"+night.key)
			span.SetTag("span.kind", "client")
			count, err := store.Count(ctx, night.id, night.start, night.end)
			span.Finish()
			if err == nil && count < 0 {
				err = fmt.Errorf("negative reserved room count for hotel %s", night.id)
			}
			if err == nil {
				cache.Set(&memcache.Item{Key: night.key, Value: []byte(strconv.Itoa(count))})
			}
			checked <- checkedNight{night.id, count, err}
		}(night)
	}
	go func() { wg.Wait(); close(checked) }()
	available := map[string]bool{}
	for _, id := range ids {
		available[id] = true
	}
	var firstErr error
	for night := range checked {
		if night.err != nil && firstErr == nil {
			firstErr = night.err
		}
		if night.count > capacities[night.id] || int(req.RoomNumber) > capacities[night.id]-night.count {
			available[night.id] = false
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	for _, id := range ids {
		if available[id] {
			result.HotelId = append(result.HotelId, id)
		}
	}
	return result, nil
}
