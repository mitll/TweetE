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


"""
Authors: Kelly Geyer
Date: April 30, 2015
Installation: Python 2.7 on Windows 7

Description: This script is an example of testing your saved API keys with pyTweet.
"""

from pyTweet import pyTweet

def main():
    # Enter proxy host and port information
    host = 'your proxy host'
    port = 'your proxy port'

    # Test keys
    pyTweet.check_twitter_key_functionality(port=port, host=host)

if __name__ == '__main__':
    main()
