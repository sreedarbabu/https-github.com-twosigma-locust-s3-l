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
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"

	"github.com/twosigma/locust-s3/locustfiles/go/locust-s3/internal/config"
	"github.com/twosigma/locust-s3/locustfiles/go/locust-s3/internal/randstr"
)

// Object operation type
const (
	Write = iota
	Read
	Delete
)

// ObjectSpec prepare an object for certain operations
type ObjectSpec struct {
	ObjectBucket string
	ObjectKey    string
	ObjectSize   int64
	ObjectData   io.ReadSeeker
	operation    int
}

const objectKeyLen = 16

var bucketCount int
var sizeWeight []string
var sizeWeightLen int

func init() {
	bucketCount = len(config.LoadConf.Data.Buckets)
	for k, v := range config.LoadConf.Data.Weights {
		var b []string
		b = make([]string, v["WEIGHT"])
		for i := 0; i < int(v["WEIGHT"]); i++ {
			b[i] = k
		}
		sizeWeight = append(sizeWeight, b...)
	}
	sizeWeightLen = len(sizeWeight)
}

func objSizeViaPolicy() int64 {
	rangePicked := rand.Intn(sizeWeightLen)
	r := config.LoadConf.Data.Weights[sizeWeight[rangePicked]]
	switch strings.ToLower(config.LoadConf.Data.SizingOption) {
	case "random":
		return int64(r["LOW"]) + int64(rand.Intn(int(r["HIGH"]-r["LOW"])))
	case "low_bound":
		return int64(r["LOW"])
	default:
		panic("unknown sizing option")
	}
}

// GetObject will initialize an object for certain operation
func (o *ObjectSpec) GetObject(operation int) error {
	switch operation {
	case Write:
		o.ObjectBucket = config.LoadConf.Data.Buckets[rand.Intn(bucketCount)]
		o.ObjectKey = fmt.Sprintf("%s%s", config.LoadConf.Data.ObjectPrefix,
			randstr.RandStringBytesMaskImprSrc(objectKeyLen))
		o.ObjectSize = objSizeViaPolicy()
		o.ObjectData = FakeObjReadSeeker(o.ObjectSize)
		o.operation = operation
		return nil
	case Read, Delete:
		o.operation = operation
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
		log.Fatalf("Object with unsupported operation %s,%s,%d", o.ObjectBucket, o.ObjectKey, o.operation)
	}
}
