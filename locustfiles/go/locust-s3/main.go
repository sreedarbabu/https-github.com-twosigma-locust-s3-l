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

package main

import (
	"bytes"
	"crypto/rand"
	"fmt"

	"github.com/twosigma/locust-s3/locustfiles/go/locust-s3/internal/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/myzhan/boomer"
)

var region = "dumpster"
var key = "test1"
var objectSize = 1024

var sharedS3Session *session.Session
var bufferBytes []byte

func initS3Session() *session.Session {
	s3Session, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(config.LoadConf.S3.Endpoint),
		Credentials:      credentials.NewStaticCredentials(config.LoadConf.S3.AccessKey, config.LoadConf.S3.AccessSecret, ""),
		Region:           aws.String(region),
		S3ForcePathStyle: aws.Bool(true)},
	)
	if err != nil {
		panic("Failed to create S3 session. please check configuration")
	}
	return s3Session
}

func initBucket() {
	svc := s3.New(sharedS3Session)
	if config.LoadConf.Data.CreateBucketOnStart {
		for _, b := range config.LoadConf.Data.Buckets {
			if _, err := svc.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case s3.ErrCodeBucketAlreadyOwnedByYou:
						fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
					default:
						panic(aerr.Error())
					}
				}
			}
		}
	}
}

func initEnvironment() {
	bufferBytes = make([]byte, objectSize, objectSize)
	if _, err := rand.Read(bufferBytes); err != nil {
		panic("could not initiate buffer")
	}
}

func getService() {
	svc := s3.New(sharedS3Session)

	start := boomer.Now()
	result, err := svc.ListBuckets(nil)
	elapsed := boomer.Now() - start

	if err != nil {
		boomer.RecordFailure("s3", "getService", elapsed, "err")
	} else {
		boomer.RecordSuccess("s3", "getService", elapsed, int64(10))
		if config.Verbose {
			for _, b := range result.Buckets {
				fmt.Printf("* %s created on %s\n",
					aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
			}
		}
	}
}

func putObject() {
	svc := s3.New(sharedS3Session)

	start := boomer.Now()
	req, _ := svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket:        aws.String(config.LoadConf.Data.Buckets[0]),
		Key:           aws.String(key),
		Body:          bytes.NewReader(bufferBytes),
		ContentLength: aws.Int64(int64(objectSize)),
		ContentType:   aws.String("binary/octet-stream"),
	})
	// Disable payload checksum calculation (very expensive)
	req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	err := req.Send()
	elapsed := boomer.Now() - start

	if err != nil {
		boomer.RecordFailure("s3", "putObject", elapsed, "err")
	} else {
		boomer.RecordSuccess("s3", "putObject", elapsed, int64(objectSize))
	}
}

func getObject() {
	svc := s3.New(sharedS3Session)

	start := boomer.Now()
	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(config.LoadConf.Data.Buckets[0]),
		Key:    aws.String(key),
	})
	elapsed := boomer.Now() - start

	if err != nil {
		boomer.RecordFailure("s3", "getObject", elapsed, "err")
	} else {
		boomer.RecordSuccess("s3", "getObject", elapsed, *resp.ContentLength)
	}
}

func deleteObject() {
	svc := s3.New(sharedS3Session)

	start := boomer.Now()
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(config.LoadConf.Data.Buckets[0]),
		Key:    aws.String(key)})
	elapsed := boomer.Now() - start

	if err != nil {
		boomer.RecordFailure("s3", "deleteObject", elapsed, "err")
	} else {
		boomer.RecordSuccess("s3", "deleteObject", elapsed, int64(10))
	}
}

func main() {
	// configuration need to be load asap
	config.LoadConf.GetConf()
	sharedS3Session = initS3Session()
	initEnvironment()
	initBucket()

	taskGetService := &boomer.Task{
		Name:   "getService",
		Weight: config.LoadConf.Ops.Weights.GetService,
		Fn:     getService,
	}
	taskPutObject := &boomer.Task{
		Name:   "putObject",
		Weight: config.LoadConf.Ops.Weights.PutObject,
		Fn:     putObject,
	}
	taskGetObject := &boomer.Task{
		Name:   "getObject",
		Weight: config.LoadConf.Ops.Weights.GetObject,
		Fn:     getObject,
	}
	taskDeleteObject := &boomer.Task{
		Name:   "deleteObject",
		Weight: config.LoadConf.Ops.Weights.DeleteObject,
		Fn:     deleteObject,
	}
	boomer.Run(taskGetService, taskGetObject, taskPutObject, taskDeleteObject)
}
