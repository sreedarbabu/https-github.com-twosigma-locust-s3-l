# coding: utf-8

# Copyright 2019 TWO SIGMA OPEN SOURCE, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
code to test S3 CRUD.

note: boto3 single thread upload API is not efficient enough. especially with sigv4
that it need to read the whole file in, compute checksum, then read it again for upload.
S3 protocol design made it not a single pass operation.

"""
import time
import inspect
import random
import logging

import uuid

import boto3
from boto3.s3.transfer import TransferConfig
import botocore
from botocore.exceptions import ClientError

from locust import Locust, TaskSet, events

from common import cache
from common import counter
from common import fakefile
from common.config import Config

# those are shortcut and since they are global and readonly, it is faster than fetch everytime
TEST_CONFIG = Config()
TEST_CONFIG_LOCUST = TEST_CONFIG['locust']
TEST_CONFIG_S3 = TEST_CONFIG['s3']
TEST_CONFIG_DATA = TEST_CONFIG['data']
TEST_COFNIG_OPS_GET_OBJ = TEST_CONFIG['ops'].setdefault('get_object', {})
TEST_CONFIG_OPS_PUT_OBJ = TEST_CONFIG['ops'].setdefault('put_object', {})


class FakeObject:
    """
    a class represent a fake object that does not have full content back by memory or disk
    """

    def __init__(self):
        self.bucket_name = random.choice(TEST_CONFIG_DATA['buckets'])
        weighted_dist = fakefile.WeightedSizeDistribution()
        self.len = weighted_dist.weighted_rand_size()
        self.len_range = weighted_dist.length_range(self.len)
        self.data = fakefile.IngressObject(self.len)
        self.obj_key = TEST_CONFIG_DATA['object_prefix'] + str(uuid.uuid4())


def get_s3_client():
    """
    helper function to get a S3 Client
    :return: s3 client
    """
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        aws_access_key_id=TEST_CONFIG_S3['access_key'],
        aws_secret_access_key=TEST_CONFIG_S3['access_secret'],
        endpoint_url=TEST_CONFIG_S3['endpoint'],
        config=botocore.config.Config(proxies=TEST_CONFIG_S3.get('proxy', {}),
                                      signature_version=TEST_CONFIG_S3['signature_version'],
                                      s3={'addressing_style': TEST_CONFIG_S3['addressing_style']})
    )
    return s3_client


def get_service(locust):
    """
    test GET Service API
    :param locust: locust context
    :return: no return value
    """
    func_name = inspect.stack()[0][3]
    try:
        start_time = time.time()
        locust.s3_client.list_buckets()
    except Exception as exception:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        new_e = Exception('{} fail after spending {} with {}'.format(func_name, total_time,
                                                                     exception))
        events.request_failure.fire(request_type="s3", name=func_name,
                                    response_time=total_time, exception=new_e)
    else:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        events.request_success.fire(request_type="s3", name=func_name,
                                    response_time=total_time, response_length=0)


def put_object(locust):
    """
    test PUT Object API
    :param locust: locust context
    :return: no return value
    """
    limits = TEST_CONFIG_OPS_PUT_OBJ.setdefault('limit', {})
    if limits.get('limited'):
        size_limit = limits.get('size_limit')
        cur_size = locust.counter.get(limits['size_counter'])
        if cur_size > size_limit:
            logging.warning('upload size limit reached')
            time.sleep(1)
            return

    obj = FakeObject()
    start_time = time.time()
    try:
        locust.s3_client.put_object(Bucket=obj.bucket_name, Key=obj.obj_key, Body=obj.data)
    except Exception as exception:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        new_e = Exception('get fail on {}/{} after spending {} with {}'.format(obj.bucket_name,
                                                                               obj.obj_key,
                                                                               total_time,
                                                                               exception))
        events.request_failure.fire(request_type="s3", name='%s-%s' % (inspect.stack()[0][3],
                                                                       obj.len_range),
                                    response_time=total_time,
                                    exception=new_e)
    else:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        events.request_success.fire(request_type="s3", name='%s-%s' % (inspect.stack()[0][3],
                                                                       obj.len_range),
                                    response_time=total_time,
                                    response_length=0)
        if TEST_CONFIG_DATA['cache_result']:
            obj_info = dict()
            obj_info['bucket'] = obj.bucket_name
            obj_info['size'] = obj.len
            if TEST_CONFIG_DATA.get('integrity_check'):
                obj_info['checksum'] = obj.data.digest()
            locust.cache.set_hm(obj.obj_key, obj_info)
        if limits.get('limited'):
            locust.counter.incrby(limits['size_counter'], obj.len)


def get_object(locust):
    """
    test GET Object API
    :param locust: locust context
    :return: no return value
    """
    obj_key, obj_info = locust.cache.get_random_hm()
    if not obj_key:
        return
    bucket_name = obj_info['bucket']
    start_time = time.time()
    len_range = 'UNKNOWN'
    try:
        check_integrity = False
        if TEST_CONFIG_DATA.get('integrity_check') and 'checksum' in obj_info:
            check_integrity = True
        obj = fakefile.EgressObject(content_digest=check_integrity)
        threading = TEST_COFNIG_OPS_GET_OBJ.get('threading', False)
        config = TransferConfig(use_threads=threading)
        locust.s3_client.download_fileobj(Bucket=bucket_name, Key=obj_key, Fileobj=obj,
                                          Config=config)
        object_length = len(obj)
        if check_integrity:
            if obj_info['checksum'] != obj.digest():
                raise Exception('obj %s got unmatched digest with %s' % (obj_key, obj.digest()))
    except Exception as exception:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        new_e = Exception('get fail on {}/{} after spending {} with {}'.format(bucket_name,
                                                                               obj_key,
                                                                               total_time,
                                                                               exception))
        events.request_failure.fire(request_type="s3", name='%s-%s' % (inspect.stack()[0][3],
                                                                       len_range),
                                    response_time=total_time,
                                    exception=new_e)
    else:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        len_range = fakefile.WeightedSizeDistribution().length_range(object_length)
        events.request_success.fire(request_type="s3", name='%s-%s' % (inspect.stack()[0][3],
                                                                       len_range),
                                    response_time=total_time,
                                    response_length=0)


def head_object(locust):
    """
    test HEAD Object API
    :param locust: locust context
    :return: no return value
    """
    obj_key, obj_info = locust.cache.get_random_hm()
    if not obj_key:
        return
    bucket_name = obj_info['bucket']
    start_time = time.time()
    try:
        locust.s3_client.head_object(Bucket=bucket_name, Key=obj_key)
    except Exception as exception:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        new_e = Exception('head fail on {}/{} after spending {} with {}'.format(bucket_name,
                                                                                obj_key,
                                                                                total_time,
                                                                                exception))
        events.request_failure.fire(request_type="s3", name=inspect.stack()[0][3],
                                    response_time=total_time, exception=new_e)
    else:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        events.request_success.fire(request_type="s3", name=inspect.stack()[0][3],
                                    response_time=total_time, response_length=0)


def delete_object(locust):
    """
    test DELETE Object API
    :param locust: locust context
    :return: no return value
    """
    obj_key, obj_info = locust.cache.get_random_hm()
    if not obj_key:
        return
    bucket_name = obj_info['bucket']
    # remove from cache asap so reduce the chance of someone else try to read it
    locust.cache.delete(obj_key)

    start_time = time.time()
    try:
        locust.s3_client.delete_object(Bucket=bucket_name, Key=obj_key)
    except Exception as exception:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        new_e = Exception('delete fail on {}/{} after spending {} with {}'.format(bucket_name,
                                                                                  obj_key,
                                                                                  total_time,
                                                                                  exception))
        events.request_failure.fire(request_type="s3", name=inspect.stack()[0][3],
                                    response_time=total_time, exception=new_e)
    else:
        total_time = int((time.time() - start_time) * TEST_CONFIG_LOCUST['time_resolution'])
        events.request_success.fire(request_type="s3", name=inspect.stack()[0][3],
                                    response_time=total_time, response_length=0)


OPS_NEED_CACHE = {get_object, head_object, delete_object}


class S3Test(TaskSet):
    """
    class to run various S3 operations. each locust virtual user has one instance of this class
    """
    cache = None

    # FIXME, do this in a cleverer way
    tasks = {
        get_service: TEST_CONFIG['ops']['weights'].get('get_service', 0),
        get_object: TEST_CONFIG['ops']['weights'].get('get_object', 0),
        put_object: TEST_CONFIG['ops']['weights'].get('put_object', 0),
        head_object: TEST_CONFIG['ops']['weights'].get('head_object', 0),
        delete_object: TEST_CONFIG['ops']['weights'].get('delete_object', 0),
    }

    def __init__(self, parent):
        super().__init__(parent)

        if TEST_CONFIG['data'].get('cache_result', False):
            self.cache = cache.KVCache()
        else:
            # somewhere locust expand the tasks into a list. has to check like this.
            if set(self.tasks) & OPS_NEED_CACHE:
                raise Exception('can not do RUD if no cache enabled')

        try:
            if TEST_CONFIG['ops']['put_object']['limit']['limited']:
                self.counter = counter.Counters()
        except Exception as _:
            pass

        self.s3_client = get_s3_client()

    def on_start(self):
        """
        do some initialization work after __init__ and before running actual tasks
        """
        if TEST_CONFIG['data'].get('create_bucket_on_start', False):
            for bucket in TEST_CONFIG['data']['buckets']:
                try:
                    self.s3_client.create_bucket(Bucket=bucket)
                except ClientError as exception:
                    if exception.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                        raise exception
        logging.info('finish start the task')


class S3Locust(Locust):
    """
    locust test class for S3
    """
    task_set = S3Test
    # those are "thinking time" in milliseconds
    min_wait = 0
    max_wait = 0
