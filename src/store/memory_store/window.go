package memory_store

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/itchyny/gojq"
	"github.com/sirupsen/logrus"
)

type Window struct {
	namespace            string
	id                   string
	granularity          int64
	current_bucket_group int64
	current              bool
	buckets              []Bucket

	db *pebble.DB

	current_bucket_group_key []byte
	bucket_keys              [][]byte

	bytes_nil []byte

	mutex sync.Mutex
}

func new_window(namespace string, id string, cardinality int64, granularity int64, current bool, db *pebble.DB) *Window {

	window := &Window{
		namespace:            namespace,
		id:                   id,
		granularity:          granularity,
		current_bucket_group: 0,
		current:              current,
		buckets:              make([]Bucket, cardinality+1),

		db: db,
	}

	if db != nil {
		window.generate_constants()
		if !window.try_load_from_db() {
			window.activate_cached_persistence()
		}
	}

	return window
}

func (window *Window) len() int64 { return int64(len(window.buckets)) }

func (window *Window) index(bucket_group int64) int64 {
	return bucket_group % window.len()
}

func (window *Window) bucket_group(timestamp int64) int64 { return timestamp / window.granularity }

func (window *Window) first_bucket_group() int64 {
	if window.current_bucket_group > window.len() {
		return window.current_bucket_group - window.len()
	} else {
		return 0
	}
}

func (window *Window) push(t int64, metric any, lambda *gojq.Code) {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	if window.bucket_group(t) >= window.first_bucket_group() {
		window._update_time(t)
		index := window.index(window.bucket_group(t))
		window.buckets[index].push(metric, lambda)

		if window.db != nil {
			if err := window.db.Set(window.bucket_keys[index], window.safe_marshal(window.buckets[index].State), pebble.NoSync); err != nil {
				logrus.Errorf("window.push unable to set new state %s %s: %v", window.namespace, window.id, err)
			}
		}
	}
}

func (window *Window) update_time(t int64) {
	window.mutex.Lock()
	defer window.mutex.Unlock()

	window._update_time(t)
}

/*
 *	Only use when a lock has been aquired beforehand
 */
func (window *Window) _update_time(t int64) {
	if window.bucket_group(t) > window.current_bucket_group {
		var min_current_bucket_group int64 = 0
		if window.bucket_group(t) > window.len() {
			min_current_bucket_group = window.bucket_group(t) - window.len()
		}

		if window.db != nil {
			batch := window.db.NewBatch()
			for bucket_group := Max(window.current_bucket_group, min_current_bucket_group) + 1; bucket_group <= window.bucket_group(t); bucket_group++ {
				index := window.index(bucket_group)
				batch.Set(window.bucket_keys[index], window.bytes_nil, nil)
			}
			batch.Set(window.current_bucket_group_key, window.safe_marshal(window.current_bucket_group), nil)

			if err := batch.Commit(pebble.NoSync); err != nil {
				logrus.Errorf("_update_time commit %s %s: %+v", window.namespace, window.id, err)
			}
		}

		for bucket_group := Max(window.current_bucket_group, min_current_bucket_group) + 1; bucket_group <= window.bucket_group(t); bucket_group++ {
			index := window.index(bucket_group)
			window.buckets[index].clear()
		}
		window.current_bucket_group = window.bucket_group(t)

	}
}

func (window *Window) check_unused() bool {
	i := int64(0)
	// safe without mutex as buckets are static
	for ; i < window.len() && window.buckets[i].State == nil; i++ {
	}
	return i == window.len()
}

func (window *Window) delete_window(batch *pebble.Batch) {
	if window.db != nil {
		batch.Delete(window.current_bucket_group_key, nil)
		for _, bucket_key := range window.bucket_keys {
			batch.Delete(bucket_key, nil)
		}
	}
}

// returns the buckets ordered
func (window *Window) get_representation() []any {
	window_rep := make([]any, 0, window.len())

	window.mutex.Lock()
	defer window.mutex.Unlock()

	logrus.Tracef("window %+v", window)

	for i := window.first_bucket_group(); i < window.current_bucket_group; i++ {
		window_rep = append(window_rep, window.buckets[window.index(i)].get_representation())
	}
	if window.current {
		window_rep = append(window_rep, window.buckets[window.index(window.current_bucket_group)].get_representation())
	}

	return window_rep
}

/*
 *	Constants
 */

func (window *Window) generate_constants() {
	// current_bucket_group key
	key_current_bucket_group := []byte(fmt.Sprintf("%s/%s/current_bucket_group", window.namespace, window.id))
	window.current_bucket_group_key = key_current_bucket_group

	// buckets key
	key_buckets := make([][]byte, window.len())
	for t := range window.buckets {
		key_buckets[t] = []byte(fmt.Sprintf("%s/%s/%d", window.namespace, window.id, t))
	}
	window.bucket_keys = key_buckets

	// bytes_nil constandt
	window.bytes_nil, _ = json.Marshal(nil)
}

/*
 *	Loads/Persistence
 */

func (window *Window) activate_cached_persistence() {
	batch := window.db.NewBatch()
	batch.Set(window.current_bucket_group_key, window.safe_marshal(0), nil)

	for _, bucket_key := range window.bucket_keys {
		batch.Set(bucket_key, window.bytes_nil, nil)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		window.db = nil
		logrus.Errorf("window activate_cached_persistence commit failed (using only memmory) %s %s: %v", window.namespace, window.id, err)
	}
}

func (window *Window) try_load_from_db() (ok bool) {

	db_current_bucket_group, closer, err := window.db.Get(window.current_bucket_group_key)
	if err == pebble.ErrNotFound {
		return false
	} else if err != nil {
		logrus.Errorf("window try_load_from_db %s %s: %+v", window.namespace, window.id, err)
		return false
	}

	var current_bucket_group int64
	window.safe_unmarshal(db_current_bucket_group, &current_bucket_group)

	if err := closer.Close(); err != nil {
		logrus.Errorf("window try_load_from_db close current_bucket_group %s %s: %+v", window.namespace, window.id, err)
	}

	if window.try_load_buckets_from_db() {
		window.current_bucket_group = current_bucket_group
		return true
	} else {
		return false
	}
}

func (window *Window) try_load_buckets_from_db() (ok bool) {
	for i := range window.buckets {
		db_bucket, closer, err := window.db.Get(window.bucket_keys[i])
		var state any
		if err != nil {
			logrus.Errorf("window try_load_buckets_from_db Get %d - %s %s: %+v", i, window.namespace, window.id, err)
			// Not need to undo the buckets already set because the current_cucket_group is 0 and will clrear the buckets before using them
			return false
		} else {
			window.safe_unmarshal(db_bucket, &state)
			if err := closer.Close(); err != nil {
				logrus.Errorf("window try_load_buckets_from_db close bucket %d - %s %s: %+v", i, window.namespace, window.id, err)
			}

			window.buckets[i].State = state
		}
	}
	return true
}

/*
 *	Marshal
 */

func (window *Window) safe_marshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		logrus.Errorf("window safe marshal %s %s: %+v", window.namespace, window.id, err)
		return []byte("")
	}
	return b
}

func (window *Window) safe_unmarshal(b []byte, v any) any {
	err := json.Unmarshal(b, v)
	if err != nil {
		logrus.Errorf("window safe unmarshal %s %s: %+v", window.namespace, window.id, err)
		return nil
	}
	return v
}

//

func Max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}
