<!--
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
-->

# Locust-S3

Locust-S3 is an AWS S3 API benchmark tool. It leverages the load test framework
[`Locust`](https://locust.io) and interacts with any S3 compatible storage using
official AWS S3 [Python](https://aws.amazon.com/sdk-for-python) and [Go](https://aws.amazon.com/sdk-for-go/) SDK.

## Key Features

- Scalable. `Locust` is a scalable load test framework.
- Highly customizable. There are many parameters could be customized.
- Open source. `Locust-S3` comes with Apache License.
- Extensible. New behaviors could be introduced by writing simple Python or Go code.

## Installation

Create a dedicated `conda` or `virtualenv` environment. Here we use `conda` as an example.

```
# conda  create -n locusts3 python=3.6 -y
# source activate locusts3
```

### Python runner installation

Install all necessary packages including the `Locust` runner.

```
# pip install -r requirements.txt
```

Repeat the same steps on each machine that will run the test.

### Go runner installation

Install locustio

```
# pip install locustio
```

Following [this](https://golang.org/doc/install) instruction on how to download and setup a Go environment. Download and build the Go runner with

```
# go get github.com/twosigma/locust-s3/locustfiles/go/locust-s3
```

## Configuration

Create a configuration yaml file. See `s3sample.yaml` for each option. Some options
could be override with environment variables.

## Test

`Locust` has many ways to run. For example, it could be run with single mode or
distributed mode. It could be run with or without Web UI. It also could save output
to different formats. Please check `Locust` [documentation](https://docs.locust.io) for more information.

### Test with Python runner

This is an example on running `Locust` locally without UI for 20 seconds with 10 clients.

```
LOCUST_CONFIG=config/getservicetest.yaml locust -f locustfiles/python/s3.py  --no-web -c 10 -r 10 -t 20s --only-summary -L WARNING --csv=res.csv
```

### Test with Go runner

Go runner needs to run in the master/slave mode.

Start a locust master with this command. (See [Locust doc](https://docs.locust.io/en/stable/quickstart.html#start-locust) for more details)

```
# locust -f ~/go/src/github.com/twosigma/locust-s3/locustfiles/go/dummy.py --master --no-web -c 10
```

Start one or more Go runners with

```
# export S3_ACCESS_KEY=<s3 access key>
# export S3_ACCESS_SECRET=<s3 access secret>
# export S3_ENDPOINT=http(s)://<s3 endpoint hostname>:<port>
# export LOCUST_CONFIG=<configuration yaml file>
# ~/go/bin/locust-s3 -master-host <hostname or ip where locust runs>
```
