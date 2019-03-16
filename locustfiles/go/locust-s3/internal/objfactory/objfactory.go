/*
Copyright 2019 TWO SIGMA OPEN SOURCE, LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package objfactory

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/twosigma/locust-s3/locustfiles/go/locust-s3/internal/config"
	"github.com/twosigma/locust-s3/locustfiles/go/locust-s3/internal/randstr"
)

type operationType int

// Object operation type
const (
	Write  operationType = 0
	Read   operationType = 1
	Delete operationType = 2
)

// ObjectSpec prepare an object for certain operations
type ObjectSpec struct {
	ObjectBucket string
	ObjectKey    string
	ObjectSize   uint64
	ObjectData   []byte
	operation    operationType
}

const objectKeyLen = 16

// GetObject will initialize an object for certain operation
func (o *ObjectSpec) GetObject(operation operationType) error {
	switch operation {
	case Write:
		o.ObjectBucket = config.LoadConf.Data.Buckets[rand.Intn(bucketCount-1)]
		o.ObjectKey = fmt.Sprintf("%s%s", config.LoadConf.Data.ObjectPrefix,
			randstr.RandStringBytesMaskImprSrc(objectKeyLen))
		o.ObjectSize = 1024
		o.ObjectData = bufferBytes
		o.operation = operation
		return nil
	case Read, Delete:
		return cacheRandomPickObject(o)
	default:
		log.Fatalf("Unsupported operation %d", o.operation)
		return nil
	}
}

// ReleaseObject will perform post processing
func (o *ObjectSpec) ReleaseObject(err error) {
	switch o.operation {
	case Write:
		if err == nil && config.LoadConf.Data.CacheResult {
			cacheAddObject(o)
		}
	case Read:
		// do nothing here.
	case Delete:
		if err == nil {
			cacheRemoveObject(o)
		}
	default:
		log.Fatalf("Obejct with unsupported operation %s,%s,%d", o.ObjectBucket, o.ObjectKey, o.operation)
	}
}

var bufferBytes []byte
var bucketCount int
var redisClient *redis.Client

// InitializeObjectFactory initialize the object factory
func InitializeObjectFactory() {
	bucketCount = len(config.LoadConf.Data.Buckets)

	bufferBytes = make([]byte, 1024, 1024)
	if _, err := rand.Read(bufferBytes); err != nil {
		panic("could not initiate buffer")
	}

	if config.LoadConf.Data.CacheResult {
		address := fmt.Sprintf("%s:%s", config.LoadConf.Cache.Server, config.LoadConf.Cache.Port)
		db, _ := strconv.ParseInt(config.LoadConf.Cache.Db, 0, 0)
		redisClient = redis.NewClient(&redis.Options{
			Addr:     address,
			Password: "",
			DB:       int(db),
		})
	}
}

func cacheAddObject(o *ObjectSpec) {
	var v = make(map[string]interface{})
	v["b"] = o.ObjectBucket
	v["k"] = o.ObjectKey
	if _, err := redisClient.HMSet(o.ObjectKey, v).Result(); err != nil {
		fmt.Printf("failed to add key to cache with %s\n", err.Error())
	}
}

func cacheRandomPickObject(o *ObjectSpec) error {
	if redisClient != nil {
		k := redisClient.RandomKey()
		if k.Err() != redis.Nil {
			if vals, err := redisClient.HMGet(k.Val(), "b", "k").Result(); err == nil {
				o.ObjectBucket = vals[0].(string)
				o.ObjectKey = vals[1].(string)
				return nil
			}
		}
	}
	return errors.New("no key from cache")
}

func cacheRemoveObject(o *ObjectSpec) {
	redisClient.Del(o.ObjectKey).Result()
}
