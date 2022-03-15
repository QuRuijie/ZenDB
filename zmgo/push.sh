#!/usr/bin/env bash

echo "build!"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -c
if [ $? != 0 ]; then exit 1; fi

echo "transport!"
scp -r -i ~/.ssh/zentertain-erase.pem ./zmgo.test  ec2-user@54.208.0.71:~/documentTest
if [ $? != 0 ]; then exit 1; fi

rm ./zmgo.test

echo "Wow Success!"