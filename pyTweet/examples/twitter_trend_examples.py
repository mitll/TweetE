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
Date: June 2, 2016
Installation: Python 2.7 on Windows 7

Description: This script is an example of using pyTweet to collecting trend information.
"""


from pyTweet import pyTweet


def main():
    # PARAMETERS
    # Enter proxy host and port information
    host = 'your proxy host'
    port = 'your proxy port'

    ##
    # AUTHENTICATE INTO THE OFFICIAL TWITTER API
    # Create proxies dictionary
    proxies = {'http': 'http://%s:%s' % (host, port), 'https': 'http://%s:%s' % (host, port)}
    # Load twitter keys
    twitter_keys = pyTweet.load_twitter_api_key_set()
    # API authorization
    OAUTH = pyTweet.get_authorization(twitter_keys)

    ##
    # PLACES THAT CONTAIN TRENDING TOPIC INFORMAION
    # Get a list of locations that Twitter has trending topic information for
    trend_locations = pyTweet.find_trend_locations(proxies=proxies, auth=OAUTH)
    print "Potential trend_locations: ", trend_locations

    ##
    # FIND TRENDS FOR A GIVEN PALCE
    # Returns the top 50 trending topics for a specific WOEID, if trending information is available for it. The response
    # is a list of 'trend' objects that encode the name of the trending topic, the query parameter that can be used to
    # search for the topic on Twitter Search, and the Twitter Search URL. Use the function find_trend_places() to obtain
    # a woeid
    #
    # PARAMETERS:
    # -----------
    #   woeid: The Yahoo! Where On Earth ID of the location to return trending information for. Global information is
    #           available by using 1 as the WOEID (default).
    #   exclude: Setting this equal to 'hashtags' will remove all hashtags from the trends list.
    woeid = trend_locations[1]['woeid']
    trends = pyTweet.get_trends_for_place(woeid=woeid, proxies=proxies, auth=OAUTH, exclude='hashtags')
    print "\nTrends for the woeid {}: ".format(woeid), trends


if __name__ == '__main__':
    main()
