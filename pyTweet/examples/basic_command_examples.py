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

Description: This script is an example of using pyTweet to collect profiles, timelines, friends and followers from the
Official Twitter API.
"""


from pyTweet import pyTweet
import datetime, os


def main():
    # PARAMETERS
    # Enter proxy host and port information
    host = ''
    port = ''
    # Load your seed of Twitter handles into the list username_seed below.
    username_seed = ['username1', 'username2']
    userid_seed = [123, 12345]
    # Start date for timeline collection: Collect tweets starting with current date back until the day specified below
    timeline_start_date = datetime.date(year=2015, month=3, day=1)
    # Sample tweet ID
    twt_id = 1234567890
    # List of links to photos and videos on Twitter
    download_media_links = []         # must have extensions 'gif', 'jpg', 'jpeg', 'jif', 'jfif', 'tif', 'tiff', 'png', 'pdf', or 'mp4'
    # Directory to save media
    media_save_dir = 'directory to save media'
    # Coordinates for Boston
    BOS = (42.3601, 71.0589)

    ##
    # AUTHENTICATE INTO THE OFFICIAL TWITTER API
    # Create proxies dictionary
    proxies = {'http': 'http://%s:%s' % (host, port), 'https': 'http://%s:%s' % (host, port)}
    # Load twitter keys
    twitter_keys = pyTweet.load_twitter_api_key_set()
    # API authorization
    OAUTH = pyTweet.get_authorization(twitter_keys)

    ##
    # CHECK KEYS
    # This function checks all of your saved Twitter API key JSON files to see if they can be used for collection
    pyTweet.check_twitter_key_functionality(host=host, port=port)

    ##
    # LOOK UP PROFILE INFORMATION
    # Returns a list of fully-hydrated user dictionaries, as specified by comma-separated values passed to the user_id
    # and/or screen_name parameters.
    #
    # There are a few things to note when using this method.
    # * You must be following a protected user to be able to see their most recent status update. If you don't follow a
    #   protected user their status will be removed.
    # * The order of user IDs or screen names may not match the order of users in the returned array.
    # * If a requested user is unknown, suspended, or deleted, then that user will not be returned in the results list.
    # * If none of your lookup criteria can be satisfied by returning a user object, a HTTP 404 will be thrown.
    #
    # PARAMETERS:
    # -----------
    #   screen_names: List of screen names, or a single one - optional
    #   user_ids: List of user IDs, or a single one - optional
    #   include_entities: The entities node that may appear within embedded statuses will be disincluded when set to false.
    user_info = pyTweet.lookup_users(proxies=proxies, auth=OAUTH, screen_names=username_seed, user_ids=userid_seed, include_entities=True)
    print user_info

    ##
    # SEARCH FOR USER PROFILES
    # Search for users after providing a string query (or list of string queries). Up to 1000 users can be returned.
    # The exclusive command indicates OR or AND usage in the query.
    #
    # PARAMETERS:
    # --------
    #   q: Search term query, must be a string object or list of string objects
    #   exclusive: Boolean, if True, search query terms with ORs rather than ANDs. Default is False
    #   limit: limit to number of users to collect. Maximum and default values are 1000
    #   include_entities: The entities node will be disincluded from embedded tweet objects when set to false.
    user_search_results1 = pyTweet.search_users(q="Twitter API", proxies=proxies, auth=OAUTH, limit=1000, exclusive=False)
    print "\nSearch result 1: ", user_search_results1
    user_search_results2 = pyTweet.search_users(q=['hangry', 'hippo'], proxies=proxies, auth=OAUTH, limit=1000, exclusive=True)
    print "\nSearch result 2: ", user_search_results2

    ##
    # LOOK UP TIME LINES
    # Find timeline of a user occuring after start_date, either from a screen name or user ID. User timelines belonging
    # to protected users may only be requested when the authenticated user either 'owns' the timeline or is an approved
    # follower of the owner. The timeline returned is the equivalent of the one seen when you view a user's profile on
    # twitter.com. This method can only return up to 3,200 of a user's most recent Tweets. Native retweets of other
    # statuses by the user is included in this total, regardless of whether include_rts is set to false when requesting
    # this resource.
    #
    # PARAMETERS:
    # -----------
    #   user_id: The ID of the user for whom to return results for.
    #   screen_name: The screen name of the user for whom to return results for.
    #   trim_user: When set to true, each tweet returned in a timeline will include a user object including only the
    #           status authors numerical ID. Omit this parameter to receive the complete user object.
    #   exclude_replies: This boolean parameter will prevent replies from appearing in the returned timeline. Using
    #           exclude_replies will mean you will receive up-to count tweets
    #   contributor_details: This boolean parameter enhances the contributors element of the status response to
    #           include the screen_name of the contributor. By default only the user_id of the contributor is included.
    #   include_rts: When set to false, the timeline will strip any native retweets (though they will still count toward
    #           both the maximal length of the timeline and the slice selected by the count parameter). Note: If you're
    #           using the trim_user parameter in conjunction with include_rts, the retweets will still contain a full
    #           user object.
    # :param start_date: start of timeline segment to collect, this is a datetime.date object. The default value is 52
    #         weeks ago from today
    tl1 = pyTweet.get_timeline(proxies=proxies, auth=OAUTH, start_date=timeline_start_date, user_id=userid_seed[0])
    print "\nTimeline for user ID: {}".format(userid_seed[0])
    print tl1
    tl2 = pyTweet.get_timeline(proxies=proxies, auth=OAUTH, start_date=timeline_start_date, screen_name=username_seed[0])
    print "\nTimeline for user screen name: {}".format(username_seed[0])
    print tl2

    ##
    # LOOK UP FRIENDS AND FOLLOWERS
    # There are two categories of API calls to collect friends and followers:
    #   1. Get lists of friends/follower IDs
    #   2. Get lists of friend/follower profile dictionaries
    #
    # Option 1 returns lists of IDs, and option 2 returns lists of profile dictionaries. These calls also vary on how
    # much information is returned per API call. You get 5,000 friend/follower IDs per call and 15 calls in a 15 minute
    # window. The later calls returning full profiles offer 20 users per call, and 15 calls in a 15 minute window.
    #
    # PARAMETERS:
    # -----------
    #   user_id: The ID of the user for whom to return results for - optional
    #   screen_name: The screen name of the user for whom to return results for - optional
    #   limit: limit to number of friends to collect. Set to None to get all friends. this is the default
    #   skip_status: When set to either true, t or 1 statuses will not be included in the returned user objects
    #   include_user_entities: The user object entities node will be disincluded when set to false.
    #
    # Get lists of friend and follower IDs
    friend_ids_list = pyTweet.get_user_friends(user_id=userid_seed[0], proxies=proxies, auth=OAUTH, limit=None)
    print "\nFriend IDs: ", friend_ids_list
    follower_ids_list = pyTweet.get_user_followers(user_id=userid_seed[0], proxies=proxies, auth=OAUTH, limit=200)
    print "\nFollower IDs: ", follower_ids_list
    # Get lists of friend and follower profile dictionaries.
    friend_profile_list = pyTweet.get_user_friend_profiles(screen_name=username_seed[0], proxies=proxies, auth=OAUTH, limit=50, include_user_entities=True)
    print "\nFriend profiles: ", friend_profile_list
    follower_profile_list = pyTweet.get_user_follower_profiles(screen_name=username_seed[1], proxies=proxies, auth=OAUTH, limit=70)
    print "\nFollower profiles: ", follower_profile_list

    ##
    # SEARCH FOR TWEETS
    # Return a list of tweets (or just one) with the following function
    #
    # PARAMETERS:
    # -----------
    #   tweet_id: Unique ID of tweet, or list of tweet IDs
    #   include_entities: The entities node that may appear within embedded statuses will be disincluded when set to false.
    #   trim_user: When set to either true, each tweet returned in a timeline will include a user object including only
    #           the status authors numerical ID. Omit this parameter to receive the complete user object.
    #   keep_missing_twts: When using the map parameter, tweets that do not exist or cannot be viewed by the current
    #           user will still have their key represented but with an explicitly null value paired with it
    twt = pyTweet.get_tweets(tweet_id=twt_id, proxies=proxies, auth=OAUTH, include_entities=True, trim_user=True, keep_missing_twts=False)
    print "\nSingle tweet: ", twt
    # This function searches for tweets based on a combination of string queries, geocode, langauge, date or result
    # types. Please note that Twitter's search service and, by extension, the Search API is not meant to be an
    # exhaustive source of Tweets. Not all Tweets will be indexed or made available via the search interface.
    #
    # PARAMETERS:
    # -----------
    #   q: A string or list of strings to query. This function searches for hashtags as well
    #   exclusive: Boolean, if True, search with ORs rather than ANDs. Default is False
    #   geocode: Returns tweets by users located within a given radius of the given latitude/longitude. The parameter
    #           value is specified by 'latitude,longitude,radius', where radius units must be specified as either 'mi'
    #           (miles) or 'km' (kilometers).
    #   lang: Restricts tweets to the given language, given by an ISO 639-1 code
    #   result_type: Specifies what type of search results you would prefer to receive. The default is 'mixed' Valid
    #           values include 'mixed' (includes both popular and real time results in the response), 'recent' (return
    #           only the most recent results in the response) and 'popular' (return only the most popular results in
    #           the response)
    #   limit: Number of tweets to collect. Set to None to get all possible tweets. The default is 100 tweets.
    #   until: Returns tweets created before the given date, which should be formatted as YYYY-MM-DD. No tweets will be
    #           found for a date older than one week.
    #   locale: Specify the language of the query you are sending (only ja is currently effective). This is intended
    #           for language-specific consumers and the default should work in the majority of cases.
    #   include_entities: The entities node will be disincluded when set to false.
    twt_list = pyTweet.search_for_tweets(proxies=proxies, auth=OAUTH, exclusive=False, geocode="{},{},5mi".format(BOS[0], BOS[1]), result_type='recent')
    print "\nTweet search results: ", twt_list
    # Search for tweets by hashtag, as well as geocode, language, result type and date. Offers the same parameters as
    # pyTweet.search_for_tweets, but query values are treated as hashtags
    hashtag_twt_list = pyTweet.get_tweets_with_hashtag(proxies=proxies, auth=OAUTH, q='#Boston', geocode="{},{},50mi".format(BOS[0], BOS[1]), exclusive=False)
    print "\nTweet hashtag search results: ", hashtag_twt_list
    # Find up to 100 retweets of a given tweet, specified by a numerical tweet ID
    #
    # PARAMETERS:
    # -----------
    #   tweet_id: Unique ID of tweet
    #   trim_user: When set to either true, t or 1, each tweet returned in a timeline will include a user object
    #           including only the status authors numerical ID. Omit this parameter to receive the complete user object.
    #   limit: limit to number of friends to collect. Max of 100 returned
    retweets = pyTweet.get_retweets(tweet_id=twt_id, proxies=proxies, auth=OAUTH, trim_user=True, limit=100)
    print '\nRetweets: ', retweets

    ##
    # DOWNLOAD MEDIA FROM TWITTER
    # Once you have a collection of tweets, you can pull out the media links, and download them using the function
    # below. The resulting dictionary link2file tells you what file the link corresponds with, if any
    #
    # PARAMETERS:
    # -----------
    #   link: A single link string, or list of link strings
    #   save_dir: Directory to save media files, default is current directory. Directory is created if it does not
    #           already exist
    link2file = pyTweet.download_tweet_media(link=download_media_links, proxies=proxies, save_dir=media_save_dir)
    print "\nThe links and corresponding files: "
    for ll in link2file:
        print "\tOriginial link: {}\n\t\tNew file: {}".format(ll, link2file[ll])


if __name__ == '__main__':
    main()

