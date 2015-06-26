#!/usr/bin/env python

#
# Copyright (c) 2015
# Massachusetts Institute of Technology
#
# All Rights Reserved
#

"""
Authors: Kelly Geyer, Andrew Heier
Date: April 30, 2015
Installation: Python 2.7 on Windows 7

        File: pyTweet.py
Installation: Python 2.7 on Windows 7
      Author: Kelly Geyer
        Date: June 24, 2015

Description: This script provides wrapper functions for the Official Twitter API. These functions enable collection of
profiles, timelines, friends and followers. During collection, the code is automatically pause for the rate limit.
"""


import requests, time, datetime, ujson, os, re, pyTweet
from requests_oauthlib import OAuth1


##
# TWITTER AUTHORIZATION
def get_twitter_certificate():
    """
    This function gets the location of Twitter API Certificate if it is stored in pyTweet's directory

    @return - Filename of Twitter API certificate, including it's path
    """
    return os.path.join(os.path.dirname(pyTweet.__file__), 'api.twitter.cer')

def get_authorization(twitter_keys):
    """
    This function obtains an authorization object for accessing the Official Twitter API.

    @param twitter_keys - Dictionary object containing 'API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET'
    @return OAUTH - Authorization object requred for remaining pyTweet collection functions
    """
    for tk in ['API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET']:
        assert (tk in twitter_keys.keys()), "The field '{}' has not been found and is required for authentication.".format(tk)
    OAUTH = OAuth1(client_key=twitter_keys['API_KEY'], client_secret=twitter_keys['API_SECRET'], resource_owner_key=twitter_keys['ACCESS_TOKEN'], resource_owner_secret=twitter_keys['ACCESS_TOKEN_SECRET'])
    return OAUTH


##
# FUNCTIONALITY FOR CHECKING THE RATE LIMIT STATUS AND PAUSING AS NEEDED
def get_rate_limit_status(type, proxies, auth):
    """
    This function returns the remaining and reset seconds.

    @param type    - Type of API call: "timeline", "friends", "followers", "search_tweets", "search_users", "retweets", or "users"
    @param proxies - proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth    - Twitter application authentication, see the get_authorization method
    @return (reset, remaining) seconds
    """
    cafile = get_twitter_certificate()
    url = 'https://api.twitter.com/1.1/application/rate_limit_status.json?resources='
    if type == 'timeline':
        rateLimitStatus = requests.get(url=url+'statuses', proxies=proxies, auth=auth, verify=cafile)
        rls_json = rateLimitStatus.json()
        reset = rls_json['resources']['statuses']['/statuses/user_timeline']['reset']
        remaining = rls_json['resources']['statuses']['/statuses/user_timeline']['remaining']
    if type == 'friends':
        rateLimitStatus = requests.get(url=url+'friends', proxies=proxies, auth=auth, verify=cafile)
        rls_json = rateLimitStatus.json()
        reset = rls_json['resources']['friends']['/friends/ids']['reset']
        remaining = rls_json['resources']['friends']['/friends/ids']['remaining']
    if type == 'followers':
        rateLimitStatus = requests.get(url=url+'followers', proxies=proxies, auth=auth, verify=cafile)
        rls_json = rateLimitStatus.json()
        reset = rls_json['resources']['followers']['/followers/ids']['reset']
        remaining = rls_json['resources']['followers']['/followers/ids']['remaining']
    if type == 'users':
        rateLimitStatus = requests.get(url=url+'users', proxies=proxies, auth=auth, verify=cafile)
        rls_json = rateLimitStatus.json()
        reset = rls_json['resources']['users']['/users/lookup']['reset']
        remaining = rls_json['resources']['users']['/users/lookup']['remaining']
    if type == 'search_tweets':
        rateLimitStatus = requests.get(url=url+'search', proxies=proxies, auth=auth, verify=cafile)
        rls_json = rateLimitStatus.json()
        reset = rls_json['resources']['search']['/search/tweets']['reset']
        remaining = rls_json['resources']['search']['/search/tweets']['remaining']
    if type == 'search_users':
        rateLimitStatus = requests.get(url=url+'search', proxies=proxies, auth=auth, verify=cafile)
        rls_json = rateLimitStatus.json()
        reset = rls_json['resources']['search']['/search/tweets']['reset']
        remaining = rls_json['resources']['search']['/search/tweets']['remaining']
    if type == 'retweets':
        rateLimitStatus = requests.get(url=url+'statuses', proxies=proxies, auth=auth, verify=cafile)
        rls_json = rateLimitStatus.json()
        reset = rls_json['resources']['statuses']['/statuses/retweets/:id']['reset']
        remaining = rls_json['resources']['statuses']['/statuses/retweets/:id']['remaining']
    return (reset, remaining)

def check_rate_limit_status(min_calls, type, auth, proxies):
    """
    This function checks the rate limit for an API call and pauses as specified by the API

    @param min_calls - minimum number of calls left before pausing
    @param type      - type of call: "timeline", "friends", "followers", "search_tweets", "search_users", "retweets", or "users"
    @param proxies   - proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth      - Twitter application authentication, see the get_authorization method
    """
    # Check parameters
    assert (type.lower() in ['timeline', 'friends', 'followers', 'users', 'search_tweets', 'search_users', 'retweets']), "You must specify the parameter type as 'timeline', 'friends', 'followers', 'users', 'search_tweets', 'search_users', or 'retweets'."
    # Collect reset and remaining
    try:
        (reset, remaining) = get_rate_limit_status(type=type, proxies=proxies, auth=auth)
    except KeyError:
        print "Pause for 15 minutes"
        time.sleep(60*15 + 20)
        (reset, remaining) = get_rate_limit_status(type=type, proxies=proxies, auth=auth)
    # Pause if needed
    if remaining < min_calls:
        pause = abs(int(time.time()) - reset) + 5
        print "There are less than ", min_calls, " calls remaining in this window. Pause for ", pause, " seconds.\n"
        time.sleep(abs(int(time.time()) - reset) + 5)


##
# FUNCTIONS FOR COLLECTING PROFILE INFORMATION
def user_lookup_usernames(user_list, proxies, auth):
    """
    Look up user information for a list of usernames. If a user's account has been deleted then it will not be returned in
    the .json of user information. We can request information for up to 100 users at a time.

    @param user_list   - list of usernames
    @param proxies    - proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth      - Twitter application authentication, see the get_authorization method
    @return user_info - list of JSON user objects that could be returned.
    """
    if (user_list is None) or (user_list == []): return []
    cafile = get_twitter_certificate()
    # Check RLS
    check_rate_limit_status(min_calls=1, type='users', proxies=proxies, auth=auth)
    # Split usernames into groups of 100
    usernames = [user_list[i:i+100] for i in range(0, len(user_list), 100)]
    # Get user information
    r = requests.get(url='https://api.twitter.com/1.1/users/lookup.json?screen_name=' + ','.join(usernames[0]),
                     proxies=proxies, auth=auth, verify=cafile)
    user_info = r.json()
    # Additional requests for user information
    if len(usernames) > 1:
        for i in range(1, len(usernames)):
            r = requests.get(url='https://api.twitter.com/1.1/users/lookup.json?screen_name=' + ','.join(usernames[i]),
                             proxies=proxies, auth=auth, verify=cafile)
            user_info = user_info + r.json()
            check_rate_limit_status(min_calls=1, type='users', proxies=proxies, auth=auth)
    return user_info

def user_lookup_userids(user_list, proxies, auth):
    """
    Look up user information for a list of user IDs. If a user's account has been deleted then it will not be returned in
    the .json of user information.  We can request information for up to 100 users at a time.

    @param user_list      - list of user ids
    @param proxies        - proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth          - Twitter application authentication, see the get_authorization method
    @return user_info     - JSON list of user information that could be retrieved
    """
    if (user_list is None) or (len(user_list) < 1): return []
    cafile = get_twitter_certificate()
    # Split user IDs into groups of 100
    userids = [user_list[i:i+100] for i in range(0, len(user_list), 100)]
    # RLS check
    check_rate_limit_status(min_calls=2, type='users', proxies=proxies, auth=auth)
    # Make API call and get user information
    str_userids = [str(j) for j in userids[0]]
    r = requests.get(url='https://api.twitter.com/1.1/users/lookup.json?user_id=' + ','.join(str_userids),
                     proxies=proxies, auth=auth, verify=cafile)
    user_info = r.json()
    # Additional requests for user information
    if len(userids) > 1:
        for i in range(len(userids)):
            str_userids = [str(j) for j in userids[i]]
            r = requests.get(url='https://api.twitter.com/1.1/users/lookup.json?user_id=' + ','.join(str_userids),
                             proxies=proxies, auth=auth, verify=cafile)
            if 'errors' in r.json(): continue
            user_info = user_info + r.json()
            check_rate_limit_status(min_calls=1, type='users', proxies=proxies, auth=auth)
    return user_info

def get_user_friends(user_id, proxies, auth, limit=None):
    """
    Look up the IDs of all of a user's friends (people they follow), and return them in a list. Find up to 5000 friends
    per request.

    @param user_id
    @param limit - limit to number of friends to collect. Set to None to get all friends - this is the default
    @param proxies - proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth - Twitter application authentication, see the get_authorization method
    @return friends_list - list of user's friends' IDs
    """
    cafile = get_twitter_certificate()
    # Set API calls based on limit
    if (limit is not None) and (int(limit) <= 5000):
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Check RLS
    check_rate_limit_status(min_calls=1, type='friends', proxies=proxies, auth=auth)
    # Call to get friends
    r = requests.get(url='https://api.twitter.com/1.1/friends/ids.json?cursor=-1&user_id={}&count=5000'.format(user_id), proxies=proxies, auth=auth, verify=cafile)
    r2 = r.json()
    friends_list = []
    try:
        friends_list = r2['ids']
        next_cursor = r2['next_cursor']
    except KeyError:
        print "Key Error in collecting followers.\n"
        return friends_list
    # Are there more friends?
    next_cursor = r2['next_cursor']
    while keepLooking and (next_cursor != 0):
        print "More than 5000 friends"
        # Check RLS
        check_rate_limit_status(min_calls=1, type='friends', proxies=proxies, auth=auth)
        # Make API call
        r = requests.get(url='https://api.twitter.com/1.1/friends/ids.json?cursor={}&user_id={}&count=5000'.format(next_cursor, user_id),
                         proxies=proxies, auth=auth, verify=cafile)
        r2 = r.json()
        friends_list = friends_list + r2['ids']
        next_cursor = int(r2['next_cursor'])
        if limit <= len(friends_list): keepLooking = False
    # Remove extra friends from the list
    if (limit is not None) and (len(friends_list) > limit): friends_list = friends_list[0:limit-1]
    return friends_list

def get_user_followers(user_id, proxies, auth, limit=None):
    """
    Look up the IDs of all of a user's followers (people who follow them), and return them in a list of json objects.

    @param user_id - User ID
    @param limit - limit to number of friends to collect. Set to None to get all friends - this is the default
    @param proxies - proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth - Twitter application authentication, see the get_authorization method
    @return followers_list - list of user's followers' IDs
    """
    cafile = get_twitter_certificate()
    # Set API calls based on limit
    if (limit is not None) and limit <= 5000:
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Check RLS
    check_rate_limit_status(min_calls=1, type='followers', proxies=proxies, auth=auth)
    # Call to get followers
    r = requests.get(url='https://api.twitter.com/1.1/followers/ids.json?cursor=-1&user_id={}&count=5000'.format(user_id), proxies=proxies, auth=auth, verify=cafile)
    r2 = r.json()
    followers_list = []
    try:
        followers_list = r2['ids']
        next_cursor = r2['next_cursor']
    except KeyError:
        print "Key Error in collecting followers.\n"
        return followers_list
    # Are there more followers?
    while keepLooking and (next_cursor != 0):
        print "More than 5000 followers.\n"
        # Check RLS
        check_rate_limit_status(min_calls=1, type='followers', proxies=proxies, auth=auth)
        # Make API call
        r = requests.get(url='https://api.twitter.com/1.1/followers/ids.json?cursor={}&user_id={}&count=5000'.format(next_cursor, user_id), proxies=proxies, auth=auth, cafile=cafile)
        r2 = r.json()
        followers_list = followers_list + r2['ids']
        # Is list complete? Has the limit been met, or does next_cursor equal 0?
        next_cursor = r2['next_cursor']
        if limit <= len(followers_list): keepLooking = False
    # Remove extra followers from list
    if (limit is not None) and (len(followers_list) > limit): followers_list = followers_list[0:limit-1]
    # Return follower list
    return followers_list


##
# FUNCTIONS FOR COLLECTING TIMELINE INFORMATION
def convert_twitter_date(twitter_date):
    """
    Convert Twitter date

    @param twitter_date - date of creation in Twitter's format
    @return converted_twitter_date - Python date object
    """
    twitter_date = twitter_date.lstrip()
    q = str(twitter_date[4:11] + twitter_date[(len(twitter_date)-4):len(twitter_date)])
    converted_twitter_date = datetime.datetime.strptime(q, "%b %d %Y").date()
    return converted_twitter_date

def collect_user_timeline(USER, USER_type, start_date, proxies, auth):
    """
    Find timeline of a user occuring after start_date.

    @param USER      - Can be either a Twitter user ID (numeric), Twitter user ID (string), or Twitter screen name.
    @param USER_type - specifies whether USER is an user ID or a screen name, enter either 'user_id' or 'screen_name'
    @param start_date- start of timeline segment to collect
    @param proxies   - proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth     - Twitter application authentication, see the get_authorization method
    @return timeline - timeline dictionary of user, None if one doesn't exist
    """
    # Determine wheter USER is a user id or user name, then return the appropriate URL
    assert ((USER_type == 'user_id') or (USER_type == 'screen_name')), "The parameter USER_type must be either 'user_id' or 'screen_name'"
    url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?{}={}&count=200'.format(USER_type, USER)
    cafile = get_twitter_certificate()
    # Collect user timeline
    check_rate_limit_status(min_calls=2, type='timeline', proxies=proxies, auth=auth)
    r = requests.get(url=url, proxies=proxies, auth=auth, verify=cafile)
    timeline = r.json()
    # Check if timeline is empty
    if len(timeline) < 1:
        print "The timeline for user ", str(USER), " is empty. Move on to next user.\n"
        return []
    # Store data in a dictionary named timeline
    try:
        max_id = timeline[len(timeline)-1]['id']
        oldest_date = convert_twitter_date(timeline[len(timeline)-1]['created_at'])
    except KeyError:
        print "Key Error within timeline. Skip this timeline.\n"
        return []
    # Continue collecting if start_date earlier than when the oldest tweet returned was published - keep collecting
    while (oldest_date-start_date).days > 0:
        # Make request for additional tweets
        r = requests.get(url='{}&max_id={}'.format(url, max_id), proxies=proxies, auth=auth, verify=cafile)
        rr2 = r.json()
        if (rr2 is None) | (len(rr2) > 1):
            # Remove redundant tweet
            rr2.remove(rr2[0])
            # Add tweets to struct
            timeline = timeline + rr2
            # update new_date and max_id
            oldest_date = convert_twitter_date(rr2[len(rr2)-1]['created_at'])
            max_id = rr2[len(rr2)-1]['id']
            # Check rate limit status for user timelines
            check_rate_limit_status(min_calls=1, type='timeline', proxies=proxies, auth=auth)
        else:
            print "There are no more tweets to collect\n"
            break
    # EOF return timeline if it exists
    return timeline

def pull_timeline_entitites(timeline, type, limit=None):
    """
    Pull fields out of a timeline array into a list.

    @param timeline - A Twitter timeline that has been loaded into Python (typically a dictionary format)
    @param type     - Specify 'user_mentions' or 'in_reply_to_user_id' to get from the timelines. Example, type = ['text', 'geo']
    @param limit    - Limit of entities to collect from timeline. Default is None which means to collect all
    @return         - List that contains the specified field, ex. List of in_reply_to_user_id.
    """
    # Check parameters
    possible_entities_to_collect = ['user_mention_id', 'in_reply_to_user_id']
    if type not in possible_entities_to_collect:
        print "The type {} is not recognized as an entity that is possible to scrape.".format(type)
        print "Please enter one of the following: " + ", ".join(possible_entities_to_collect)
        return []
    if (timeline is None) or (len(timeline) < 1): return []
    # Find entities
    entities = set([])
    for ii in range(len(timeline)):
        if (type == 'in_reply_to_user_id') and (timeline[ii]['in_reply_to_user_id'] is not None):
            entities.add(int(timeline[ii]['in_reply_to_user_id']))
        if (type == 'user_mention_id') and (timeline[ii]['entities']['user_mentions'] is not []) and (timeline[ii]['entities']['user_mentions'] is not None):
            mentions = timeline[ii]['entities']['user_mentions']
            for jj in range(len(mentions)):
                entities.add(mentions[jj]['id'])
        # Check limit
        if (limit is not None) and (len(entities) > limit): return entities[0:limit]
    return entities


version = '1.0'
# End of pyTweet.py
