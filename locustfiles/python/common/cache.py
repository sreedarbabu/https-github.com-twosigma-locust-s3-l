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
a simple REDIS based cache
"""
import logging

import redis
from redis.exceptions import RedisError

from .config import Config


class KVCache:
    """
    simple wrapper around Redis
    """

    def __init__(self):
        config = Config()['cache']
        self.cache_stub = redis.StrictRedis(
            host=config['server'], port=config['port'], db=config['db'], decode_responses=True)

    def set(self, key, value):
        """
        set key with value
        :param key:
        :param value:
        :return: no return value
        """
        self.cache_stub.set(key, value)

    def delete(self, key):
        """
        delete the key from the cache
        :param key:
        :return: no return value
        """
        self.cache_stub.delete(key)

    def get_random_item(self):
        """
        get a random item
        :return: a random key with its value
        """
        key = self.cache_stub.randomkey()
        value = None
        if key:
            value = self.cache_stub.get(key)
        return key, value

    def set_hm(self, key, value):
        """
        set a key with a hashmap (dictionary)
        :param key: the key to set
        :param value: the value (dictionary)
        :return: no return value
        """
        self.cache_stub.hmset(key, value)

    def get_random_hm(self):
        """
        get a random key and associated hashmap value
        :return: key and associated hashmap as a dictionary.
        """
        try:
            key = self.cache_stub.randomkey()
            data = self.cache_stub.hgetall(key)
        except RedisError as exception:
            logging.exception(exception)
            return None, None
        else:
            return key, data
