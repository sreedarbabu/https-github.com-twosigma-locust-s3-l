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
	"strconv"

	"github.com/go-redis/redis"
	"github.com/twosigma/locust-s3/locustfiles/go/locust-s3/internal/config"
)

var redisClient *redis.Client

func init() {
	if config.LoadConf.Data.CacheResult {
		address := fmt.Sprintf("%s:%s", config.LoadConf.Cache.Server, config.LoadConf.Cache.Port)
		db, _ := strconv.ParseInt(config.LoadConf.Cache.Db, 0, 0)
		redisClient = redis.NewClient(&redis.Options{
			Addr:     address,
			Password: "",
			DB:       int(db),
		})
		if _, err := redisClient.Ping().Result(); err != nil {
			log.Fatalf("failed to connect to redis with %s\n", err.Error())
		}
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
	if redisClient == nil {
		log.Fatalln("no cache enabled at all")
	} else {
		k := redisClient.RandomKey()
		if k.Err() != redis.Nil {
			if vals, err := redisClient.HMGet(k.Val(), "b", "k").Result(); err == nil {
				o.ObjectBucket = vals[0].(string)
				o.ObjectKey = vals[1].(string)
				return nil
			}
		}
		return errors.New("no key from cache")
	}
	return errors.New("no key from cache")
}

func cacheRemoveObject(o *ObjectSpec) {
	redisClient.Del(o.ObjectKey).Result()
}
