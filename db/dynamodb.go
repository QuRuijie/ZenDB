package db

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

var gDynamodbSvr *dynamodb.DynamoDB
var gS3Svr *s3.S3

func GetDynamodbSvr() *dynamodb.DynamoDB {
	return gDynamodbSvr
}

func GetS3Svr() *s3.S3 {
	return gS3Svr
}

func InitDynamodb(accessKeyId string, secretAccessKey string) {
	credConf := credentials.NewStaticCredentials(accessKeyId, secretAccessKey, "")
	awsConf := aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: credConf,
	}

	// Create the session that the DynamoDB service will use.
	sess, err := session.NewSession(&awsConf)
	if err != nil {
		panic(fmt.Sprintf("create databoat session err:%+v", err))
	}

	// Create the DynamoDB service client to make the put item request with.
	gDynamodbSvr = dynamodb.New(sess)
}

func InitS3Svr(accessKeyId string, secretAccessKey string) {
	credConf := credentials.NewStaticCredentials(accessKeyId, secretAccessKey, "")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credConf,
	})

	if err != nil {
		panic(fmt.Sprintf("create s3 session err:%+v", err))
	}

	// Create S3 service client
	gS3Svr = s3.New(sess)
}
