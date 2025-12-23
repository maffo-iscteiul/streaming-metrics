package memory_store

import (
	"encoding/json"

	"github.com/itchyny/gojq"
	"github.com/sirupsen/logrus"
)

type Bucket struct {
	State any `json:"state"`
	//mutex sync.Mutex
}

func (bucket *Bucket) push(metric any, lambda *gojq.Code) {
	// bucket.mutex.Lock()
	// defer bucket.mutex.Unlock()

	iter := lambda.Run(nil, bucket.State, metric)
	v, ok := iter.Next()
	if !ok {
		logrus.Errorf("Bucket.push: lambda function did not return new state")
		return
	}
	if _, ok := v.(error); ok {
		logrus.Errorf("Bucket.push: %+v", v.(error))
		return
	} else {
		bucket.State = v
	}
}

// TODO improve the deepcopy mecanism
func (bucket *Bucket) get_representation() any {
	// bucket.mutex.Lock()
	bucket_bytes, err := json.Marshal(bucket)
	// bucket.mutex.Unlock()

	if err != nil {
		logrus.Errorf("gojq_bucket marshal: %+v", err)
		return nil
	}

	bucket_rep := make(map[string]any)

	if err := json.Unmarshal(bucket_bytes, &bucket_rep); err != nil {
		logrus.Errorf("gojq_bucket unmarshal: %+v", err)
		return nil

	}

	return bucket_rep["state"]
}

func (bucket *Bucket) clear() {
	// bucket.mutex.Lock()
	// defer bucket.mutex.Unlock()
	bucket.State = nil
}
