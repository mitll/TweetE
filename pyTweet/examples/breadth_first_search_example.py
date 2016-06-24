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

Description: This script is an example of using pyTweet to create a Twitter data set using breadth-first sampling.
"""


import datetime
from pyTweet import breadth_first_sampling


def main():
    # Enter proxy host and port information
    host = 'your proxy host'
    port = 'your proxy port'

    # Beginning dates of time lines that you plan to collect. These timelines will range from the start_date to the current date
    tl_start_date = datetime.date(year=2015, month=3, day=1)

    # Set locations for the profile and timeline directory to save .JSONs. The default will be your current working
    # directory.
    save_dir = {'twitter_profiles': 'path/to/twitter/profile/json/files/dir',
                'twitter_timelines': 'path/to/twitter/timelines/json/files/dir'}

    # Specify your graph constrains with the variable hop_out_limits. First determine the maximum number of hops to make
    # the graph with 'max_hops', then decide the maximum amount of data to collect in 'max_data'. This will be the
    # combined profile and timeline .JSON files. Set it to 'None' if you don't want to limit the amount of data
    # collected. Next, Set limits (per individual) on how many friends, followers, replied to users, and mentioned
    # users to include on the next hop. You can specify values [0, Inf) or None. Specifying 'None' implies that you do
    # not wish to limit the collection, and will expand the graph on as many as these edges as possible. Occasionlly,
    # you may get back fewer edges for a user than the limit you set. Note that friends and followers will be saved in
    # the fields 'friends_list' and 'followers_list' automatically. The reply and mention users are saved in timelines.
    hop_limits = {'max_hops': 5,                # Maximum number of hops in graph
                  'max_data': 50,                # Maximum amount of data (in GB)
                  'friends': 0,                 # Maximum friends per user to include in next hop
                  'followers': 0,               # Maximum followers per user to include in next hop
                  'in_reply_to_user_id': 0,  # Maximum 'in_reply_to_user_id' per user's timeline to include in next hop
                  'user_mention_id': 5}      # Maximum 'user_mention_id' per user's timeline to include in next hop

    # Load your seed of Twitter handles into the list username_seed below.
    username_seed = ['username1', 'username2', 'username3']

    # Build network and get user information
    print "\nBuild network and get user information"
    breadth_first_sampling.breadth_first_search(user_seed=username_seed,
                                 timeline_start_date=tl_start_date,
                                 host=host, port=port,
                                 save_dir=save_dir,
                                 hop_limits=hop_limits)


if __name__ == '__main__':
    main()
