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
a simple class to implement a file like object to provide read only stream
"""
import logging
import os
import random
import numpy
import xxhash

from .config import Config


class IngressObject:
    """
    A virtual read only file like object
    """
    # class variables so no need to redo this for every instance
    SEED_LENGTH = 1048576
    DIGEST_BLOCK_SIZE = 1048576
    seed = numpy.random.bytes(SEED_LENGTH)

    def __init__(self, size=0):
        self.size = size
        self.offset = 0
        # TODO: introduce a salt here. so that even we reuse the same buffer at class level,
        # each instance has its own salt so that even same size data will has different content.
        # self.salt_offset = random.randint(0, self.SEED_LENGTH)

    def tell(self):
        """
        :return: current offset
        """
        return self.offset

    def seek(self, offset, from_what=os.SEEK_SET):
        """
        adjust offset
        :param offset: offset value
        :param from_what: seek option. see os module
        :return: no return value
        """
        if from_what == os.SEEK_SET:
            self.offset = offset
        elif from_what == os.SEEK_CUR:
            self.offset += offset
        elif from_what == os.SEEK_END:
            self.offset = self.size + offset
        else:
            raise IOError('Invalid argument')
        if self.offset < 0:
            raise IOError('Invalid argument')

    def read(self, size=-1):
        """
        return data up to size
        :param size:
        :return: data read
        """
        if self.offset >= self.size:
            # return EOF
            return bytearray(0)
        # read no more than what is left
        if size == -1:
            read_size = self.size - self.offset
        else:
            read_size = min(self.size - self.offset, size)
        # read no more than what left in the seed (from the offset)
        offset_in_seed = self.offset % self.SEED_LENGTH
        read_size = min(read_size, self.SEED_LENGTH - offset_in_seed)
        self.offset += read_size
        return self.seed[offset_in_seed:offset_in_seed + read_size]

    def __len__(self):
        return self.size

    def digest(self):
        """
        helper code to return the digest of full content
        """
        old_offset = self.tell()
        self.seek(0, os.SEEK_SET)
        hash_value = xxhash.xxh64()
        while True:
            data_block = self.read(self.DIGEST_BLOCK_SIZE)
            if not data_block:
                break
            hash_value.update(data_block)
        self.seek(old_offset, os.SEEK_SET)
        return hash_value.hexdigest()


class EgressObject:
    """
    a virtual write only file like object
    """

    def __init__(self, content_digest=False):
        self.size = 0
        self.offset = 0
        if content_digest:
            self.__digest = xxhash.xxh64()
        else:
            self.__digest = None

    def write(self, data):
        """
        write data
        :param data: data to be written
        :return: no return value
        """
        if self.__digest:
            self.__digest.update(data)
        self.size = self.size + len(data)

    def __len__(self):
        return self.size

    def digest(self):
        """
        :return: digest of the written data
        """
        return self.__digest.hexdigest()


def generate_weights(size_distribution):
    """
    :param size_distribution:
    :return: a list that contains ranges with weights.
    """
    w_list = []
    for key, value in size_distribution.items():
        w_list.extend([key] * value['WEIGHT'])
    return w_list


class WeightedSizeDistribution:
    """
    a helper class to support weighted size distribution.
    """
    config = None
    # size distribution (weight, low bound, high bound)
    size_distribution = None
    weighted_size_distribution = None

    def __init__(self):
        """
        initialize class variable
        """
        if not self.config:
            self.config = Config()
            # size distribution (weight, low bound, high bound)
            self.size_distribution = self.config['data']['weights']
            self.weighted_size_distribution = generate_weights(self.size_distribution)

    def length_range(self, length):
        """
        return the range that represents the length
        :param length: value to identify the range
        :return: range string
        """
        for key, value in self.size_distribution.items():
            if value['LOW'] <= length < value['HIGH']:
                return key
        logging.debug('a large object %d', length)
        return 'LARGE'

    def weighted_rand_size(self):
        """
        generate a weighted random size
        :return: a weighted random size
        """
        slot = random.choice(self.weighted_size_distribution)
        option = self.config['data'].get('sizing_option', 'random')
        if option == 'low_bound':
            size = self.size_distribution[slot]['LOW']
        elif option == 'random':
            size = numpy.random.randint(self.size_distribution[slot]['LOW'],
                                        high=self.size_distribution[slot]['HIGH'])
        else:
            raise Exception('unsupported sizing option')
        return size
