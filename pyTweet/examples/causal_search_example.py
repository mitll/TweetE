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

Description: ...
"""

import datetime
from pyTweet import *

import os


def main():
    # Params for connecting to the PostgreSQL database
    postgres_params = {'dbname': 'your database name',
                       'user': 'role name',
                       'password': 'role password',
                       'host': 'your host',
                       'port': 'your port'}

    # Enter proxy information
    host = 'proxy host'
    port = 'proxy port'

    # Enter time frame for time line
    tl_start_date = datetime.date(year=2016, month=1, day=1)
    tl_end_date = datetime.date(year=2016, month=7, day=1)

    # Enter the maximum number of hops
    hop_limits = {'max_hops': 5}

    # Enter the directory to save place saving variables that keep track of the collection in case the process is
    # interrupted. You should never delete these files.
    save_params = {'twitter_profiles': '/dir/to/save/place/saving/variables'}

    # Load seed of users
    print '\nLoad username seed'
    username_seed = ['username1', 'username2', 'username3']


    # Load seed of keywords
    keyword_seed = ['yolo', 'pyTweet']

    # Prepare new database
    print "\nPrepare new database"
    json_to_database.prepare_graph_database(postgres_params=postgres_params)
    json_to_database.clear_tables(postgres_params)
    pyTweet.clear_place_savers(user_dir=save_params['twitter_profiles'])

    depth_first_sampling.depth_first_causal_search(user_seed=username_seed,
                                      topic_seed=keyword_seed,
                                      tl_start_date=tl_start_date, tl_end_date=tl_end_date,
                                      postgres_params=postgres_params,
                                      host=host, port=port,
                                      save_dir=save_params,
                                      hop_limits=hop_limits, collection_limits={})

if __name__ == '__main__':
    main()