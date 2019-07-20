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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/twosigma/locust-s3/locustfiles/go/locust-s3/internal/config"
	"github.com/twosigma/locust-s3/locustfiles/go/locust-s3/internal/objfactory"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/myzhan/boomer"
)

var region = "dumpster"

var sharedServiceClient *s3.S3

func initS3Client() *s3.S3 {
	s3Session, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(config.LoadConf.S3.Endpoint),
		Credentials:      credentials.NewStaticCredentials(config.LoadConf.S3.AccessKey, config.LoadConf.S3.AccessSecret, ""),
		Region:           aws.String(region),
		S3ForcePathStyle: aws.Bool(true)},
	)
	if err != nil {
		panic("Failed to create S3 session. please check configuration")
	}
	// see https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/custom-http.html
	// on why use a custom http client
	svc := s3.New(s3Session, &aws.Config{HTTPClient: &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   100,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}})
	return svc
}

func initBuckets() {
	if config.LoadConf.Data.CreateBucketOnStart {
		for _, b := range config.LoadConf.Data.Buckets {
			if _, err := sharedServiceClient.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
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

func getService() {

	start := time.Now().UnixNano() / config.LoadConf.Locust.TimeResolution
	result, err := sharedServiceClient.ListBuckets(nil)
	elapsed := time.Now().UnixNano()/config.LoadConf.Locust.TimeResolution - start

	if err != nil {
		boomer.RecordFailure("s3", "getService", elapsed, err.Error())
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
	var obj objfactory.ObjectSpec
	if err := obj.GetObject(objfactory.Write); err != nil {
		time.Sleep(1000 * time.Millisecond)
		return
	}

	start := time.Now().UnixNano() / config.LoadConf.Locust.TimeResolution
	req, _ := sharedServiceClient.PutObjectRequest(&s3.PutObjectInput{
		Bucket:        aws.String(obj.ObjectBucket),
		Key:           aws.String(obj.ObjectKey),
		Body:          obj.ObjectData,
		ContentLength: aws.Int64(int64(obj.ObjectSize)),
		ContentType:   aws.String("binary/octet-stream"),
	})
	// Disable payload checksum calculation (very expensive)
	req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	err := req.Send()
	elapsed := time.Now().UnixNano()/config.LoadConf.Locust.TimeResolution - start

	if err != nil {
		boomer.RecordFailure("s3", "putObject", elapsed, err.Error())
		if config.Verbose {
			fmt.Printf("put object %s/%s with size %d fail\n", obj.ObjectBucket, obj.ObjectKey, obj.ObjectSize)
		}
	} else {
		boomer.RecordSuccess("s3", "putObject", elapsed, int64(obj.ObjectSize))
		if config.Verbose {
			fmt.Printf("put object %s/%s with size %d succ\n", obj.ObjectBucket, obj.ObjectKey, obj.ObjectSize)
		}
	}
	obj.ReleaseObject(err)
}

func withAcceptEncoding(e string) request.Option {
	return func(r *request.Request) {
		r.HTTPRequest.Header.Add("Accept-Encoding", e)
	}
}

func getObject() {
	var obj objfactory.ObjectSpec
	if err := obj.GetObject(objfactory.Read); err != nil {
		if config.Verbose {
			fmt.Println("no object for get operation from cache, will sleeep 1 sec and retry")
		}
		time.Sleep(1000 * time.Millisecond)
		return
	}

	ctx := context.Background()

	start := time.Now().UnixNano() / config.LoadConf.Locust.TimeResolution
	resp, err := sharedServiceClient.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(obj.ObjectBucket),
		Key:    aws.String(obj.ObjectKey),
	}, withAcceptEncoding("identity"))
	if err != nil {
		elapsed := time.Now().UnixNano()/config.LoadConf.Locust.TimeResolution - start
		boomer.RecordFailure("s3", "getObject", elapsed, err.Error())
	} else {
		defer resp.Body.Close()
		length, err := io.Copy(ioutil.Discard, resp.Body)
		elapsed := time.Now().UnixNano()/config.LoadConf.Locust.TimeResolution - start
		if err == nil {
			boomer.RecordSuccess("s3", "getObject", elapsed, int64(length))
			if config.Verbose {
				fmt.Printf("get object %s/%s\n", obj.ObjectBucket, obj.ObjectKey)
			}
		} else {
			boomer.RecordFailure("s3", "getObject", elapsed, fmt.Sprintf("get %s/%s failed with %s", obj.ObjectBucket, obj.ObjectKey, err.Error()))
		}
	}
	obj.ReleaseObject(err)
}

func headObject() {

	var obj objfactory.ObjectSpec
	if err := obj.GetObject(objfactory.Read); err != nil {
		if config.Verbose {
			fmt.Println("no object for head operation from cache, will sleeep 1sec and retry")
		}
		time.Sleep(1000 * time.Millisecond)
		return
	}

	start := time.Now().UnixNano() / config.LoadConf.Locust.TimeResolution
	resp, err := sharedServiceClient.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(obj.ObjectBucket),
		Key:    aws.String(obj.ObjectKey),
	})
	elapsed := time.Now().UnixNano()/config.LoadConf.Locust.TimeResolution - start

	if err != nil {
		boomer.RecordFailure("s3", "headObject", elapsed, err.Error())
	} else {
		boomer.RecordSuccess("s3", "headObject", elapsed, *resp.ContentLength)
		if config.Verbose {
			fmt.Printf("head object %s/%s\n", obj.ObjectBucket, obj.ObjectKey)
		}
	}
	obj.ReleaseObject(err)
}

func deleteObject() {

	var obj objfactory.ObjectSpec
	if err := obj.GetObject(objfactory.Delete); err != nil {
		if config.Verbose {
			fmt.Println("no object for delete operation from cache, will sleeep 1sec and retry")
		}
		time.Sleep(1000 * time.Millisecond)
		return
	}
	time.Sleep(time.Duration(config.LoadConf.Locust.TimeDelay) * time.Millisecond)

	start := time.Now().UnixNano() / config.LoadConf.Locust.TimeResolution
	_, err := sharedServiceClient.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(obj.ObjectBucket),
		Key:    aws.String(obj.ObjectKey)})
	elapsed := time.Now().UnixNano()/config.LoadConf.Locust.TimeResolution - start

	if err != nil {
		boomer.RecordFailure("s3", "deleteObject", elapsed, err.Error())
	} else {
		boomer.RecordSuccess("s3", "deleteObject", elapsed, int64(10))
		if config.Verbose {
			fmt.Printf("delete object %s/%s\n", obj.ObjectBucket, obj.ObjectKey)
		}
	}
	obj.ReleaseObject(err)
}

func main() {
	sharedServiceClient = initS3Client()

	initBuckets()

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
	taskHeadObject := &boomer.Task{
		Name:   "headObject",
		Weight: config.LoadConf.Ops.Weights.HeadObject,
		Fn:     headObject,
	}
	boomer.Run(taskGetService, taskGetObject, taskPutObject, taskDeleteObject, taskHeadObject)
}
