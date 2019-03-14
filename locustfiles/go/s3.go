package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/myzhan/boomer"
)

var verbose = false
var region = "dumpster"
var accessKey = os.Getenv("LOCUST_S3_ACCESS_KEY")
var accessSecret = os.Getenv("LOCUST_S3_ACCESS_SECRET")
var endPoint = os.Getenv("LOCUST_S3_ENDPOINT")
var bucket = "locust-s3-benchmark-bucket"
var key = "test1"
var objectSize = 1024

var sharedS3Session *session.Session
var bufferBytes []byte

func initS3Session() *session.Session {
	s3Session, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endPoint),
		Credentials:      credentials.NewStaticCredentials(accessKey, accessSecret, ""),
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
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
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

func initEnvironment() {
	bufferBytes = make([]byte, objectSize, objectSize)
	_, err := rand.Read(bufferBytes)
	if err != nil {
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
		if verbose {
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
		Bucket:        aws.String(bucket),
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
		Bucket: aws.String(bucket),
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
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	elapsed := boomer.Now() - start

	if err != nil {
		boomer.RecordFailure("s3", "deleteObject", elapsed, "err")
	} else {
		boomer.RecordSuccess("s3", "deleteObject", elapsed, int64(10))
	}
}

func main() {
	sharedS3Session = initS3Session()
	initEnvironment()
	initBucket()

	taskGetService := &boomer.Task{
		Name:   "getService",
		Weight: 1,
		Fn:     getService,
	}
	taskPutObject := &boomer.Task{
		Name:   "putObject",
		Weight: 1,
		Fn:     putObject,
	}
	taskGetObject := &boomer.Task{
		Name:   "getObject",
		Weight: 1,
		Fn:     getObject,
	}
	taskDeleteObject := &boomer.Task{
		Name:   "deleteObject",
		Weight: 1,
		Fn:     deleteObject,
	}
	boomer.Run(taskGetService, taskGetObject, taskPutObject, taskDeleteObject)
}
