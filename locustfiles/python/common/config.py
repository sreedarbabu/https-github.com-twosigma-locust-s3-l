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
build configuratin from default values, configuration file, and environment variable
"""
import os

import anyconfig
from humanfriendly import parse_size


class Config:
    """
    configuration
    """

    # to get a singleton instance
    class SingletonConfig:
        """
        A helper class to help with the Singleton pattern. No need to parse configuration
        multiple times
        """
        DEFAULT_CONFIG = """
locust :
  # report time in microsecond
  time_resolution : 1000000

s3:
  signature_version : s3
  addressing_style : path
  access_key : foo
  access_secret : bar

data :
  cache_result : False
        """

        def __init__(self):
            self.config = anyconfig.loads(self.DEFAULT_CONFIG, ac_parser='yaml')

            file_name = os.environ.get('LOCUST_CONFIG')
            if not file_name or not os.path.isfile(file_name):
                raise Exception('invalid test configuration for locust, '
                                'check LOCUST_CONFIG environment variable.')
            anyconfig.merge(self.config, anyconfig.load(file_name, ac_parser='yaml'))

            self.config['s3']['endpoint'] = os.getenv('S3_ENDPOINT',
                                                      self.config['s3']['endpoint'])
            self.config['s3']['endpoint'] = [u.strip() for u in self.config['s3']['endpoint'].split(',')]
            self.config['s3']['access_key'] = os.getenv('S3_ACCESS_KEY',
                                                        self.config['s3']['access_key'])
            self.config['s3']['access_secret'] = os.getenv('S3_ACCESS_SECRET',
                                                           self.config['s3']['access_secret'])
            if 'cache' in self.config:
                self.config['cache']['server'] = os.getenv('LT_CACHE_SERVER',
                                                           self.config['cache'].get('server'))
                self.config['cache']['port'] = os.getenv('LT_CACHE_SERV_PORT',
                                                         self.config['cache'].get('port'))
                self.config['cache']['db'] = os.getenv('LT_CACHE_SERV_DB',
                                                       self.config['cache'].get('db'))

            try:
                config_weights = self.config['data']['weights']
                for value in config_weights.values():
                    value['LOW'] = parse_size(str(value['LOW']))
                    value['HIGH'] = parse_size(str(value['HIGH']))
            except KeyError:
                pass

            try:
                self.config['ops']['put_object']['limit']['size_limit'] = parse_size(
                    str(self.config['ops']['put_object']['limit']['size_limit']))
            except KeyError:
                pass

    instance = None

    def __init__(self):
        if not Config.instance:
            Config.instance = Config.SingletonConfig()

    def __getattr__(self, name):
        return getattr(self.instance.config, name)

    def __getitem__(self, item):
        return self.instance.config[item]
