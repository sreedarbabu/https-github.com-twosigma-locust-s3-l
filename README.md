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
official AWS S3 Python SDK [`Boto3`](https://aws.amazon.com/sdk-for-python).

## Key Features

- Scalable. `Locust` is a scalable load test framework.
- Highly customizable. There are many parameters could be customized.
- Open source. `Locust-S3` comes with Apache License.
- Extensible. New behaviors could be introduced by writing simple python code.

## Installation

Create a dedicated `conda` or `virtualenv` environment. Here we use `conda` as an example.

```
# conda  create -n locusts3 python=3.6 -y
# source activate locusts3
```

Install all necessary packages including the `Locust` runner.

```
# pip install -r requirements.txt
```

Repeat the same steps on each machine that will run the test.

## Configuration

Create a configuration yaml file. See `s3sample.yaml` for each option. Some options
could be override with environment variables.

## Test

`Locust` has many ways to run. For example, it could be run with single mode or
distributed mode. It could be run with or without Web UI. It also could save output
to different formats. Please check `Locust` [documentation](https://docs.locust.io) for more information.

This is an example on running `Locust` locally without UI for 20 seconds with 10 clients.

```
LOCUST_CONFIG=config/getservicetest.yaml locust -f locustfiles/s3.py  --no-web -c 10 -r 10 -t 20s --only-summary -L WARNING --csv=res.csv
```
