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

Description: This script is an example of populating a PostgreSQL database with Twitter profile and timeline JSON files,
 as well as creating edges (a whole new table called 'graph') for the pyTweet database.
"""


from pyTweet import *


def main():
    ##
    # PARAMETERS
    # Params for connecting to the PostgreSQL database
    postgres_params = {'dbname': 'name of database',
                       'user': 'role name',
                       'password': 'postgres password',
                       'host': 'postgres host',
                       'port': 'postgres port'}
    # Directory containing profile JSON files
    profiles = "path/to/profile/json/files"
    # Directory containing timeline JSON files
    timelines = "path/to/timeline/json/files"

    ##
    # PREPARE DATABASE
    # Create database
    json_to_database.prepare_graph_database(postgres_params)
    # Clear database
    json_to_database.clear_tables(postgres_params)

    ##
    # POPULATE DATABASE WITH PROFILE AND TIMELINE JSON FILES
    # Add profile metadata from JSON files
    json_to_database.load_user_information(postgres_params=postgres_params, user_dir=profiles)
    # Add timeline metadata from JSON files
    json_to_database.load_timelines_information(postgres_params=postgres_params, timeline_dir=timelines)
    # Add edges - alter edge_types to contain as many types as you wish. This example creates all possible edges.
    json_to_database.load_edges(postgres_params, edge_types=[1, 2, 3, 4, 5, 6], add_missing_users=False)
    # Add hashtag edges: Link users who use the same hashtag
    json_to_database.load_hashtag_edges(postgres_params=postgres_params, hashtag='indieweb')


if __name__ == '__main__':
    main()