#!/usr/bin/env python

#
# Copyright (c) 2015
# Massachusetts Institute of Technology
#
# All Rights Reserved
#

"""
Authors: Kelly Geyer
Installation: Python 2.7 on Windows 7

        File: pyTweet.py
Installation: Python 2.7 on Windows 7
      Author: Kelly Geyer
        Date: June 24, 2015

Description: This script is an example of using pyTweet to create a Twitter dataset with breadth first sampling.
"""

import pyTweet, os, datetime


def main():
	# PARAMETERS
    # Enter proxy host and port information
    host = 'your.host'
    port = 'your.port'
    # Beginning dates of time lines that you plan to collect. These timelines will range from the start_date to the current date
    start_date = datetime.date(year=2015, month=3, day=1)
    # Set locations for the profile and timeline directory to save .JSONs. The default will be your current working
    # directory.
    save_dir = {'twitter_profiles': '',
                'twitter_timelines': ''}
				
	# SAMPLING OPTIONS
    # Specify your graph constrains with the variable hop_out_limits. First determine the maximum number of hops to make
    # the graph with 'max_hops', then decide the maximum amount of data to collect in 'max_data'. This will be the
    # combined profile and timeline .JSON files. Set it to 'None' if you don't want to limit the amount of data
    # collected. Next, Set limits (per individual) on how many friends, followers, replied to users, and mentioned
    # users to include on the next hop. You can specify values [0, Inf) or None. Specifying 'None' implies that you do
    # not wish to limit the collection, and will expand the graph on as many as these edges as possible. Occasionlly,
    # you may get back fewer edges for a user than the limit you set. Note that friends and followers will be saved in
    # the fields 'friends_list' and 'followers_list' automatically. The reply and mention users are saved in timelines.
    hop_out_limits = {'max_hops': 5,                # Maximin number of hops in graph
                      'max_data': 10,                # Maximum amount of data (in GB)
                      'friends': None,               # Maximum friends per user to include in next hop
                      'followers': None,             # Maximum followers per user to include in next hop
                      'in_reply_to_user_id': 1,  # Maximum 'in_reply_to_user_id' per user's timeline to include in next hop
                      'user_mention_id': 1}      # Maximum 'user_mention_id' per user's timeline to include in next hop
    # Suppose that you want to store friends or followers, but do not want to expand the graph based on them. Specify
    # limitations on collecting friends and followers below. Notice that reply and mention users are saved in the
    # timelines. The largest possible length of 'friends_list' will be the greater of hops out limit and collection
    # limit, or MAX(hops_out_limit['friends'], collection_limits['friends']). The same description follows for
    # 'followers_list'.
    collection_limits = {'friends': None,          # Maximum number of friends per user to save within the profile .JSON
                         'followers': 200}      # Maximum number of followers per user to save within the profile .JSON
    # Load your seed of Twitter handles into the list username_seed below. It's not necessary to include the '@'
    username_seed = ['user1', 'user2', 'user3']

    # BUILD NETWORK
    print "\nBuild network and get user information"
    pyTweet.breadth_first_search(user_seed=username_seed, timeline_start_date=start_date, host=host, port=port, save_dir=save_dir, hop_out_limits=hop_out_limits, collection_limits=collection_limits)


if __name__ == '__main__':
    main()
