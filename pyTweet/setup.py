#!/usr/bin/env python


#
# Copyright 2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with
# the License.
#
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
#


import os, re
from setuptools import setup


def readme():
    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.md")) as fid:
        return fid.read()

def do_setup():
    setup(name = 'pyTweet',
      version = 'dev',
      author = 'MIT Lincoln Laboratory',
      url = 'https://github.com/mitll/TweetE',
      description = 'A Python wrapper, sampler, and certificate manager for the Twitter API',
      long_description = readme(),
      license = 'Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0',
      packages = ['pyTweet'],
      platforms = ['Windows', 'Linux', 'Mac OS-X', 'Unix'])


if __name__ == "__main__":
    do_setup()