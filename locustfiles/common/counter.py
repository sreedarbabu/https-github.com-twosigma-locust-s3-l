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
a simple redis based global counter
"""
import redis

from .config import Config


class Counters:
    """
    simple wrapper around Redis
    """

    def __init__(self):
        config = Config()['counter']
        self.stub = redis.StrictRedis(
            host=config['server'], port=config['port'], db=config['db'])

    def get(self, key):
        """
        get the value by key
        :param key: key which identify the counter
        :return: the value by the key
        """
        value = self.stub.get(key)
        if value:
            value = int(value.decode("utf-8"))
        else:
            value = 0
        return value

    def incrby(self, key, cnt):
        """
        increase the counter by certain value atomically
        :param key: key which identify the counter
        :param cnt: the amount to be increased
        :return: the value after increment
        """
        return self.stub.incrby(key, cnt)
