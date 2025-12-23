package memory_store

import (
	"encoding/json"
	"fmt"
	"sync"

	store_interface "example.com/streaming-metrics/src/store"

	"github.com/cockroachdb/pebble"
	"github.com/itchyny/gojq"
	"github.com/sirupsen/logrus"
)

var (
	global_db_mutex sync.Mutex
	global_db       *pebble.DB = nil
)

type Memory_store struct {
	namespace      string
	granularity    int64
	cardinality    int64
	snapshot       int64
	current        bool
	current_time   int64
	windows        map[string]*Window
	windows_idx_db map[string]int
	idx_windows_db map[int]string

	db *pebble.DB

	current_time_key []byte

	rwmutex sync.RWMutex
}

func New_memory_store(namespace string, granularity int64, cardinality int64, snapshot int64, current bool) store_interface.Store {
	if !valid_memory_inputs(namespace, granularity, cardinality, snapshot, current) {
		return nil
	}

	return &Memory_store{
		namespace:   namespace,
		granularity: granularity,
		cardinality: cardinality,
		snapshot:    snapshot,
		current:     current,
		db:          nil,
		windows:     make(map[string]*Window),
	}
}

func New_cached_persistent_store(namespace string, granularity int64, cardinality int64, snapshot int64, current bool) store_interface.Store {
	if !valid_memory_inputs(namespace, granularity, cardinality, snapshot, current) {
		return nil
	}

	global_db_mutex.Lock()
	if global_db == nil {
		t_db, err := pebble.Open("persistent_data", &pebble.Options{})
		if err != nil {
			logrus.Errorf("memory new_cached_persistent_store open pebble db %s: %+v", namespace, err)
			global_db_mutex.Unlock()
			return nil
		}
		global_db = t_db
	}
	db := global_db
	global_db_mutex.Unlock()

	memory := &Memory_store{
		namespace:      namespace,
		granularity:    granularity,
		cardinality:    cardinality,
		snapshot:       snapshot,
		current:        current,
		db:             db,
		windows:        make(map[string]*Window),
		windows_idx_db: make(map[string]int),
		idx_windows_db: make(map[int]string),
	}

	memory.generate_constants()

	if !memory.try_load_from_db() {
		memory.activate_cached_persistence()
	}

	return memory
}

func (store *Memory_store) Tick(t int64) {
	store.rwmutex.RLock()
	if store.current_time < t {
		//TODO mutex arround this update (is iteven necessary? - unlikely to be concurrency and temporary incorrect value is good enough)
		for _, window := range store.windows {
			window.update_time(t)
		}
		store.current_time = t
		if store.db != nil {
			store.db.Set(store.current_time_key, store.safe_marshal(store.current_time), pebble.NoSync)
		}
	}
	store.rwmutex.RUnlock()

	// TODO this does not need to be checked every tick
	store.check_and_remove_unused_windows()
}

func (store *Memory_store) Push(id string, t int64, metric any, lambda *gojq.Code) {
	store.rwmutex.RLock()

	window, ok := store.windows[id]
	if ok {
		window.push(t, metric, lambda)
	} else {
		store.rwmutex.RUnlock()
		store.rwmutex.Lock()
		if _, ok := store.windows[id]; !ok {
			if store.db != nil {
				batch := store.db.NewBatch()
				batch.Set(store.len_windows_key(), store.safe_marshal(len(store.windows)+1), nil)
				batch.Set(store.window_idx_key(len(store.windows)), store.safe_marshal(id), nil)

				if err := batch.Commit(pebble.NoSync); err != nil {
					logrus.Errorf("store Push commit failed %s: %+v", store.namespace, err)
				}

				store.windows_idx_db[id] = len(store.windows)
				store.idx_windows_db[len(store.windows)] = id
			}
			store.windows[id] = new_window(store.namespace, id, store.cardinality, store.granularity, store.current, store.db)
		}
		store.rwmutex.Unlock()
		store.rwmutex.RLock()
		window := store.windows[id]
		window.push(t, metric, lambda)
	}
	store.rwmutex.RUnlock()
}

func (store *Memory_store) check_and_remove_unused_windows() {
	store.rwmutex.RLock()
	unused_windows := store._check_unused_windows()
	store.rwmutex.RUnlock()

	if len(unused_windows) == 0 {
		return
	}

	store.rwmutex.Lock()
	defer store.rwmutex.Unlock()

	batch := store.db.NewBatch()
	for _, id := range unused_windows {
		if window := store.windows[id]; window != nil {
			if window.check_unused() {
				if store.db != nil {
					last_id := store.idx_windows_db[len(store.windows)-1]
					del_idx := store.windows_idx_db[id]

					batch.Set(store.len_windows_key(), store.safe_marshal(len(store.windows)-1), nil)
					batch.Set(store.window_idx_key(del_idx), store.safe_marshal(last_id), nil)
					batch.Delete(store.window_idx_key(len(store.windows)-1), nil)

					store.idx_windows_db[del_idx] = last_id
					store.windows_idx_db[last_id] = del_idx

					delete(store.idx_windows_db, len(store.windows)-1)
					delete(store.windows_idx_db, id)

					window.delete_window(batch)
				}
				delete(store.windows, id)
			}
		}
	}

	if store.db != nil {
		if err := batch.Commit(pebble.NoSync); err != nil {
			logrus.Errorf("memory check_and_remove_unused_windows commit %s: %+v", store.namespace, err)
		}
	}
}

// requires at least a RLock
func (store *Memory_store) _check_unused_windows() []string {
	unused_ids := make([]string, 0, len(store.windows))

	for id, window := range store.windows {
		if window.check_unused() {
			unused_ids = append(unused_ids, id)
		}
	}
	return unused_ids
}

func (store *Memory_store) Get_representation() (map[string]any, int64) {
	store_rep := make(map[string]any)
	store.rwmutex.RLock()

	logrus.Tracef("memory store %+v", store)

	for window_id, window := range store.windows {
		store_rep[window_id] = window.get_representation()
	}
	store.rwmutex.RUnlock()
	return store_rep, store.current_time
}

func valid_memory_inputs(namespace string, granularity int64, cardinality int64, snapshot int64, current bool) bool {
	return len(namespace) > 0 && granularity > 0 && cardinality > 0 && snapshot > 0
}

/*
 *	BD Keys
 */

func (store *Memory_store) len_windows_key() []byte {
	return []byte(fmt.Sprintf("%s/len_windows", store.namespace))
}

func (store *Memory_store) granularity_key() []byte {
	return []byte(fmt.Sprintf("%s/granularity", store.namespace))
}

func (store *Memory_store) cardinality_key() []byte {
	return []byte(fmt.Sprintf("%s/cardinality", store.namespace))
}

func (store *Memory_store) window_idx_key(index int) []byte {
	return []byte(fmt.Sprintf("%s/window/%d", store.namespace, index))
}

func (store *Memory_store) generate_constants() {
	store.current_time_key = []byte(fmt.Sprintf("%s/current_time", store.namespace))
}

/*
 *	Loads/Persistence
 */

func (store *Memory_store) activate_cached_persistence() {
	logrus.Infof("memmory try_load_from_db %s: Persiste namespace", store.namespace)
	batch := store.db.NewBatch()
	batch.Set(store.granularity_key(), store.safe_marshal(store.granularity), nil)
	batch.Set(store.cardinality_key(), store.safe_marshal(store.cardinality), nil)
	batch.Set(store.len_windows_key(), store.safe_marshal(0), nil)
	batch.Set(store.current_time_key, store.safe_marshal(0), nil)

	if err := batch.Commit(pebble.NoSync); err != nil {
		store.db = nil
		logrus.Errorf("memory activate_cached_persistence commit failed (using only memmory) %s: %v", store.namespace, err)
	}
}

func (store *Memory_store) try_load_from_db() (ok bool) {

	bd_granularity, closer_gran, err_gran := store.db.Get(store.granularity_key())
	bd_cardinality, closer_card, err_card := store.db.Get(store.cardinality_key())
	defer func() {
		if closer_gran != nil {
			if err := closer_gran.Close(); err != nil {
				logrus.Errorf("memory try_load_from_db close granularity %s: %+v", store.namespace, err)
			}
		}
		if closer_card != nil {
			if err := closer_card.Close(); err != nil {
				logrus.Errorf("memory try_load_from_db close cardinality %s: %+v", store.namespace, err)
			}
		}
	}()

	if err_gran == pebble.ErrNotFound || err_card == pebble.ErrNotFound {
		logrus.Debugf("memmory try_load_from_db %s: Namespace not persisted to pebble DB yet.", store.namespace)
		return false
	} else if err_gran != nil || err_card != nil {
		logrus.Errorf("memmory try_load_from_db %s: err_gran %+v err_card %+v", store.namespace, err_gran, err_card)
		return false
	}

	var my_gran, my_card int64
	store.safe_unmarshal(bd_granularity, &my_gran)
	store.safe_unmarshal(bd_cardinality, &my_card)

	if my_gran != store.granularity || my_card != store.cardinality {
		logrus.Errorf("memmory try_load_from_db %s: Config: {granularity:%d, cardinality:%d} BD: {granularity:%d, cardinality:%d}", store.namespace, store.granularity, store.cardinality, my_gran, my_card)
		return false
	}

	return store.try_load_windows_from_db()
}

func (store *Memory_store) try_load_windows_from_db() (ok bool) {

	bd_current_time, closer, err := store.db.Get(store.current_time_key)
	if err != nil {
		logrus.Errorf("memory try_load_windows_from_db %s - Unable to get store current time: %+v", store.namespace, err)
		return false
	}
	var current_time int64
	store.safe_unmarshal(bd_current_time, &current_time)

	if err := closer.Close(); err != nil {
		logrus.Errorf("memory try_load_windows_from_db close current_time %s: %+v", store.namespace, err)
	}

	bd_number_windows, closer, err := store.db.Get(store.len_windows_key())
	if err != nil {
		logrus.Errorf("memory try_load_windows_from_db %s - Unable to get number of windows: %+v", store.namespace, err)
		return false
	}
	var number_windows int
	store.safe_unmarshal(bd_number_windows, &number_windows)

	if err := closer.Close(); err != nil {
		logrus.Errorf("memory try_load_windows_from_db close number_windows_key %s: %+v", store.namespace, err)
	}
	for idx := 0; idx < number_windows; idx++ {
		bd_id, closer, err := store.db.Get(store.window_idx_key(idx))
		if err != nil {
			logrus.Errorf("memory try_load_windows_from_db %s - Unable to get windows idx %d: %+v", store.namespace, idx, err)
			for k := range store.windows {
				delete(store.windows, k)
			}
			for k := range store.windows_idx_db {
				delete(store.windows_idx_db, k)
			}
			for k := range store.idx_windows_db {
				delete(store.idx_windows_db, k)
			}
			return false
		}
		var id string
		store.safe_unmarshal(bd_id, &id)
		if err := closer.Close(); err != nil {
			logrus.Errorf("store try_load_windows_from_db close window id %d - %s: %+v", idx, store.namespace, err)
		}
		store.windows_idx_db[id] = idx
		store.idx_windows_db[idx] = id

		store.windows[id] = new_window(store.namespace, id, store.cardinality, store.granularity, store.current, store.db)
	}

	store.current_time = current_time

	return true
}

/*
 *	Marshal
 */

func (store *Memory_store) safe_marshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		logrus.Errorf("memory_store safe marshal %s: %+v", store.namespace, err)
		return []byte("")
	}
	return b
}

func (store *Memory_store) safe_unmarshal(b []byte, v any) any {
	err := json.Unmarshal(b, v)
	if err != nil {
		logrus.Errorf("memory_store safe unmarshal %s: %+v", store.namespace, err)
		return nil
	}
	return v
}
