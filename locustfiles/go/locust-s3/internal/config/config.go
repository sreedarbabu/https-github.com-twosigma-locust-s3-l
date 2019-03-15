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

package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/tonnerre/golang-pretty"
	"gopkg.in/yaml.v2"
)

// Verbose with true will lead to more verbose debug message
var Verbose = true

// Go unfortunately has quite poort YAML parsing support.
// have to paste sample.yaml to https://mengzhuo.github.io/yaml-to-go/ to get this structure
// also would like to map Weights to map of map like map[string]map[string]string `yaml:"weights"`

// LocustS3Configuration is the corresponding struct for configuration
type LocustS3Configuration struct {
	Locust struct {
		TimeResolution int `yaml:"time_resolution"`
	} `yaml:"locust"`
	Cache struct {
		Server string `yaml:"server"`
		Port   string `yaml:"port"`
		Db     string `yaml:"db"`
	} `yaml:"cache"`
	Counter struct {
		Server string `yaml:"server"`
		Port   string `yaml:"port"`
		Db     string `yaml:"db"`
	} `yaml:"counter"`
	S3 struct {
		Endpoint         string `yaml:"endpoint"`
		SignatureVersion string `yaml:"signature_version"`
		AddressingStyle  string `yaml:"addressing_style"`
		AccessKey        string `yaml:"access_key"`
		AccessSecret     string `yaml:"access_secret"`
	} `yaml:"s3"`
	Data struct {
		CacheResult         bool                         `yaml:"cache_result"`
		IntegrityCheck      bool                         `yaml:"integrity_check"`
		Buckets             []string                     `yaml:"buckets"`
		CreateBucketOnStart bool                         `yaml:"create_bucket_on_start"`
		ObjectPrefix        string                       `yaml:"object_prefix"`
		SizingOption        string                       `yaml:"sizing_option"`
		Weights             map[string]map[string]uint32 `yaml:"weights"`
	} `yaml:"data"`
	Ops struct {
		Weights struct {
			GetService   int `yaml:"get_service"`
			PutObject    int `yaml:"put_object"`
			GetObject    int `yaml:"get_object"`
			HeadObject   int `yaml:"head_object"`
			DeleteObject int `yaml:"delete_object"`
		} `yaml:"weights"`
		GetObject struct {
			Threading bool `yaml:"threading"`
		} `yaml:"get_object"`
		PutObject struct {
			Limit struct {
				Limited     bool   `yaml:"limited"`
				SizeCounter string `yaml:"size_counter"`
				SizeLimit   string `yaml:"size_limit"`
			} `yaml:"limit"`
		} `yaml:"put_object"`
	} `yaml:"ops"`
}

// GetConf will load configuration
func (c *LocustS3Configuration) GetConf() *LocustS3Configuration {
	var yamlFile []byte
	var err error
	if yamlFile, err = ioutil.ReadFile(os.Getenv("LOCUST_CONFIG")); err != nil {
		log.Fatalf("yamlFile.Get err   #%v ", err)
	}
	if err = yaml.Unmarshal(yamlFile, c); err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	var value string
	var present bool
	if value, present = os.LookupEnv("LT_CACHE_SERVER"); present {
		c.Cache.Server = value
	}
	if value, present = os.LookupEnv("LT_CACHE_SERV_PORT"); present {
		c.Cache.Port = value
	}
	if value, present = os.LookupEnv("LT_CACHE_SERV_DB"); present {
		c.Cache.Port = value
	}
	if value, present = os.LookupEnv("S3_ENDPOINT"); present {
		c.S3.Endpoint = value
	}
	if value, present = os.LookupEnv("S3_ACCESS_KEY"); present {
		c.S3.AccessKey = value
	}
	if value, present = os.LookupEnv("S3_ACCESS_SECRET"); present {
		c.S3.AccessSecret = value
	}

	/* FIXME bytefmt has difficulty to parse these stuff. giving up for now.
	will need strict numerics in the yaml file.
	WeightedSizeConf = make(map[string]WeightedSizeRange)

	for k, v := range c.Data.Weights {
		var high, low uint64
		var weight uint64
		var err error
		log.Printf("Try to convert %v, %v", k, v["HIGH"])
		if high, err = bytefmt.ToBytes(v["HIGH"]); err != nil {
			log.Fatalf("Failed to convert %v", err)
		}
		log.Printf("Try to convert %v, %v", k, v["LOW"])
		if low, err = bytefmt.ToBytes(v["LOW"]); err != nil {
			log.Fatalf("Failed to convert %v", err)
		}
		log.Printf("Try to convert %v, %v", k, v["WEIGHT"])
		if weight, err = strconv.ParseUint(v["WEIGHT"], 0, 64); err != nil {
			log.Fatalf("Failed to convert %v", err)
		}
		WeightedSizeConf[k] = WeightedSizeRange{high: high, low: low, weight: weight}
	}
	*/
	if Verbose {
		fmt.Printf("%+v\n", pretty.Formatter(c))
	}
	return c
}

// LoadConf will hold an immutable copy of configuration
var LoadConf LocustS3Configuration
