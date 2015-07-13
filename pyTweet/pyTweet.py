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


import requests, time, datetime, ujson, os, re, pyTweet, urllib2
from requests_oauthlib import OAuth1


##
# TWITTER AUTHORIZATION
def get_twitter_certificate(proxies):
    """
    This function gets the location of Twitter API Certificate if it is stored in pyTweet's directory. If the .cer file cannot be found it will be created

    :param proxies:
    :return cafile: Filename of Twitter API certificate, including it's path
    """
    cafile = os.path.join(os.path.dirname(pyTweet.__file__), 'api.twitter.cer')     # Twitter API CA Certificate
    cert_url = 'http://curl.haxx.se/ca/cacert.pem'                                  # CA certificate site
    if not os.path.isfile(cafile):
        print "\tThe file {} cannot be found...downloading it from {}.".format(cafile, cert_url)
        file = open(cafile, 'wb')
        file.write(urllib2.urlopen(cert_url).read())
        file.close()
    return cafile

def load_twitter_api_key_set():
    """
    This funcitons loads a set of Twitter keys.

    :return twitter_keys: Dictionary object containing 'API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET'
    """
    key_dir = os.path.join(os.path.dirname(pyTweet.__file__), 'twitter_api_keys')
    key_jsons = os.listdir(key_dir)
    key_jsons = filter(lambda k: re.match('.+\.json', k), key_jsons)
    key_jsons = [os.path.join(key_dir, j) for j in key_jsons]
    key = {}
    for k in range(len(key_jsons)):
        key = ujson.loads(open(key_jsons[k]).read())
        if ('API_KEY' in key.keys()) and ('API_SECRET' in key.keys()) and ('ACCESS_TOKEN' in key.keys()) and ('ACCESS_TOKEN_SECRET' in key.keys()):
            print "\tLoad the keys from file {}.".format(key_jsons[k])
            return key
        else:
            print "\tWarning! The file {} does not contain a valid Twitter API key. Please refer to the documentation on creating an API key".format(key_jsons[k])
    assert (key != {}), "None of the Twitter key JSONs were formatted properly - refer to the documentation on creating key files."
    return key

def get_authorization(twitter_keys):
    """
    This function obtains an authorization object for accessing the Official Twitter API.

    :param twitter_keys: Dictionary object containing 'API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET'
    :return OAUTH: Authorization object requred for remaining pyTweet collection functions
    """
    for tk in ['API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET']:
        assert (tk in twitter_keys.keys()), "The field '{}' has not been found and is required for authentication.".format(tk)
    OAUTH = OAuth1(client_key=twitter_keys['API_KEY'], client_secret=twitter_keys['API_SECRET'], resource_owner_key=twitter_keys['ACCESS_TOKEN'], resource_owner_secret=twitter_keys['ACCESS_TOKEN_SECRET'])
    return OAUTH

def change_twitter_keys(type, reset, remaining, limit, proxies, auth):
    """
    This function switches to another pair of Twitter API keys, if they are available, to avoid pausing.

    :param type:
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method

    :return new_auth: Authorization object using new_keys
    :return isNewAuth: Boolean value representing whether a new authorization has been produced
    """
    # Count JSON files in key directory
    key_dir = os.path.join(os.path.dirname(pyTweet.__file__), 'twitter_api_keys')
    key_jsons = os.listdir(key_dir)
    key_jsons = filter(lambda k: re.match('.+\.json', k), key_jsons)
    key_jsons = [os.path.join(key_dir, j) for j in key_jsons]
    isNewAuth = False
    # Check if content is valid enough to continue...
    assert (len(key_jsons) > 0), "You have no Twitter API key files saved in {}. Refer to the documentation to create key files.".format(key_dir)
    if len(key_jsons) == 1:
        print "\tThere are no other API keys to use...returning current API key."
        pause = abs(int(time.time()) - reset) + 5
        print "\tThere are no alternative keys. Pause for {} seconds.".format(pause)
        time.sleep(pause)
        return (auth, isNewAuth)
    best_key_auth = auth
    best_key = {}
    best_key[type] = {'RESET': reset, 'LIMIT': limit, 'REMAINING': remaining}
    for k in range(len(key_jsons)):
        try:
            key = ujson.loads(open(key_jsons[k]).read())
        except ValueError:
            print "\tWarning! The file {} does not contain a valid Twitter API key. Please refer to the documentation on creating an API key".format(key_jsons[k])
            continue
        # Be sure the file contains a valid Twitter key
        if ('API_KEY' not in key.keys()) or ('API_SECRET' not in key.keys()) or ('ACCESS_TOKEN' not in key.keys()) or ('ACCESS_TOKEN_SECRET' not in key.keys()):
            print "\tWarning! The file {} does not contain a valid Twitter API key. Please refer to the documentation on creating an API key".format(key_jsons[k])
            continue
        key_auth = get_authorization(key)
        (reset2, remaining2, limit2) = get_rate_limit_status(type=type, proxies=proxies, auth=key_auth)
        key[type] = {'RESET': reset2, 'LIMIT': limit2, 'REMAINING': remaining2}
        # Check keys!
        if key[type]['REMAINING'] == key[type]['LIMIT']:
            best_key = key
            best_key_auth = key_auth
            isNewAuth = True
            break
        if (key[type]['REMAINING'] > best_key[type]['REMAINING']):
            best_key = key
            best_key_auth = key_auth
            isNewAuth = True
    # Create Twitter authorization
    if best_key[type]['REMAINING'] < 1:
        pause = abs(int(time.time()) - best_key[type]['RESET']) + 5
        print "\tThere are no alternative keys. Pause for {} minutes.".format(pause/60)
        time.sleep(pause)
    return (best_key_auth, isNewAuth)


##
# FUNCTIONALITY FOR CHECKING THE RATE LIMIT STATUS AND PAUSING AS NEEDED
def get_rate_limit_status(type, proxies, auth):
    """
    This function returns the remaining and reset seconds.

    @param type       - Type of API call: "timeline", "friends", "followers", "search_tweets", "search_users", "retweets", or "users"
    @param proxies    - proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth       - Twitter application authentication, see the get_authorization method
    @return reset - The remaining window before the limit resets in UTC epoch seconds
    @return remaining - the number of requests left for the 15 minute window
    @return limit - the rate limit ceiling for that given request
    """
    cafile = get_twitter_certificate(proxies)
    url = 'https://api.twitter.com/1.1/application/rate_limit_status.json?resources='
    try:
        if type == 'timeline':
            rateLimitStatus = requests.get(url=url+'statuses', proxies=proxies, auth=auth, verify=cafile)
            rls_json = rateLimitStatus.json()
            reset = rls_json['resources']['statuses']['/statuses/user_timeline']['reset']
            remaining = rls_json['resources']['statuses']['/statuses/user_timeline']['remaining']
            limit = rls_json['resources']['statuses']['/statuses/user_timeline']['limit']
        if type == 'friends':
            rateLimitStatus = requests.get(url=url+'friends', proxies=proxies, auth=auth, verify=cafile)
            rls_json = rateLimitStatus.json()
            reset = rls_json['resources']['friends']['/friends/ids']['reset']
            remaining = rls_json['resources']['friends']['/friends/ids']['remaining']
            limit = rls_json['resources']['friends']['/friends/ids']['limit']
        if type == 'followers':
            rateLimitStatus = requests.get(url=url+'followers', proxies=proxies, auth=auth, verify=cafile)
            rls_json = rateLimitStatus.json()
            reset = rls_json['resources']['followers']['/followers/ids']['reset']
            remaining = rls_json['resources']['followers']['/followers/ids']['remaining']
            limit = rls_json['resources']['followers']['/followers/ids']['limit']
        if type == 'users':
            rateLimitStatus = requests.get(url=url+'users', proxies=proxies, auth=auth, verify=cafile)
            rls_json = rateLimitStatus.json()
            reset = rls_json['resources']['users']['/users/lookup']['reset']
            remaining = rls_json['resources']['users']['/users/lookup']['remaining']
            limit = rls_json['resources']['users']['/users/lookup']['limit']
        if type == 'search_tweets':
            rateLimitStatus = requests.get(url=url+'search', proxies=proxies, auth=auth, verify=cafile)
            rls_json = rateLimitStatus.json()
            reset = rls_json['resources']['search']['/search/tweets']['reset']
            remaining = rls_json['resources']['search']['/search/tweets']['remaining']
            limit = rls_json['resources']['search']['/search/tweets']['limit']
        if type == 'search_users':
            rateLimitStatus = requests.get(url=url+'search', proxies=proxies, auth=auth, verify=cafile)
            rls_json = rateLimitStatus.json()
            reset = rls_json['resources']['search']['/search/tweets']['reset']
            remaining = rls_json['resources']['search']['/search/tweets']['remaining']
            limit = rls_json['resources']['search']['/search/tweets']['limit']
        if type == 'retweets':
            rateLimitStatus = requests.get(url=url+'statuses', proxies=proxies, auth=auth, verify=cafile)
            rls_json = rateLimitStatus.json()
            reset = rls_json['resources']['statuses']['/statuses/retweets/:id']['reset']
            remaining = rls_json['resources']['statuses']['/statuses/retweets/:id']['remaining']
            limit = rls_json['resources']['statuses']['/statuses/retweets/:id']['limit']
    except KeyError:
        return (time.time() + 900, 0, 15)
    return (reset, remaining, limit)

def check_rate_limit_status(min_calls, type, auth, proxies):
    """
    This function checks the rate limit for an API call and pauses as specified by the API

    @param min_calls - minimum number of calls left before pausing
    @param type      - type of call: "timeline", "friends", "followers", "search_tweets", "search_users", "retweets", or "users"
    @param proxies   - proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    @param auth      - Twitter application authentication, see the get_authorization method
    @return auth
    """
    # Check parameters
    assert (type.lower() in ['timeline', 'friends', 'followers', 'users', 'search_tweets', 'search_users', 'retweets']), "You must specify the parameter type as 'timeline', 'friends', 'followers', 'users', 'search_tweets', 'search_users', or 'retweets'."
    # Collect reset and remaining
    # try:
    #     (reset, remaining, limit) = get_rate_limit_status(type=type, proxies=proxies, auth=auth)
    # except KeyError:
    #     (auth, isNewAuth) = change_twitter_keys(type=type, reset=(time.time() + 900), remaining=0, limit=15, proxies=proxies, auth=auth)
    #     (reset, remaining, limit) = get_rate_limit_status(type=type, proxies=proxies, auth=auth)
    (reset, remaining, limit) = get_rate_limit_status(type=type, proxies=proxies, auth=auth)
    # Pause if needed
    if remaining < min_calls:
        pause = abs(int(time.time()) - reset) + 5
        # If pause is longer than 3 minutes try another set of keys...
        if pause > 300:
            (auth, isNewAuth) = change_twitter_keys(type=type, reset=reset, remaining=remaining, limit=limit, proxies=proxies, auth=auth)
        else:
            print "There are less than {} calls remaining in this window. Pause for {} seconds.".format(min_calls, pause)
            time.sleep(pause)
    return auth


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
    cafile = get_twitter_certificate(proxies)
    # Check RLS
    auth = check_rate_limit_status(min_calls=1, type='users', proxies=proxies, auth=auth)
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
            auth = check_rate_limit_status(min_calls=1, type='users', proxies=proxies, auth=auth)
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
    cafile = get_twitter_certificate(proxies)
    # Split user IDs into groups of 100
    userids = [user_list[i:i+100] for i in range(0, len(user_list), 100)]
    # RLS check
    auth = check_rate_limit_status(min_calls=2, type='users', proxies=proxies, auth=auth)
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
            auth = check_rate_limit_status(min_calls=1, type='users', proxies=proxies, auth=auth)
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
    cafile = get_twitter_certificate(proxies)
    # Set API calls based on limit
    if (limit is not None) and (int(limit) <= 5000):
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Check RLS
    auth = check_rate_limit_status(min_calls=1, type='friends', proxies=proxies, auth=auth)
    # Call to get friends
    r = requests.get(url='https://api.twitter.com/1.1/friends/ids.json?cursor=-1&user_id={}&count=5000'.format(user_id), proxies=proxies, auth=auth, verify=cafile)
    r2 = r.json()
    friends_list = []
    try:
        friends_list = r2['ids']
        next_cursor = r2['next_cursor']
    except KeyError:
        print "\tKey Error in collecting followers."
        return friends_list
    # Are there more friends?
    next_cursor = r2['next_cursor']
    while keepLooking and (next_cursor != 0):
        print "\tMore than 5000 friends..."
        # Check RLS
        auth = check_rate_limit_status(min_calls=1, type='friends', proxies=proxies, auth=auth)
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
    cafile = get_twitter_certificate(proxies)
    # Set API calls based on limit
    if (limit is not None) and limit <= 5000:
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Check RLS
    auth = check_rate_limit_status(min_calls=1, type='followers', proxies=proxies, auth=auth)
    # Call to get followers
    r = requests.get(url='https://api.twitter.com/1.1/followers/ids.json?cursor=-1&user_id={}&count=5000'.format(user_id), proxies=proxies, auth=auth, verify=cafile)
    r2 = r.json()
    followers_list = []
    try:
        followers_list = r2['ids']
        next_cursor = r2['next_cursor']
    except KeyError:
        print "Key Error in collecting followers."
        return followers_list
    # Are there more followers?
    while keepLooking and (next_cursor != 0):
        print "\tMore than 5000 followers..."
        # Check RLS
        auth = check_rate_limit_status(min_calls=1, type='followers', proxies=proxies, auth=auth)
        # Make API call
        r = requests.get(url='https://api.twitter.com/1.1/followers/ids.json?cursor={}&user_id={}&count=5000'.format(next_cursor, user_id), proxies=proxies, auth=auth, verify=cafile)
        r2 = r.json()
        print 'r2: ', r2.keys()
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
    cafile = get_twitter_certificate(proxies)
    # Collect user timeline
    auth = check_rate_limit_status(min_calls=2, type='timeline', proxies=proxies, auth=auth)
    r = requests.get(url=url, proxies=proxies, auth=auth, verify=cafile)
    timeline = r.json()
    # Check if timeline is empty
    if len(timeline) < 1:
        print "The timeline for user ", str(USER), " is empty. Move on to next user."
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
            auth = check_rate_limit_status(min_calls=1, type='timeline', proxies=proxies, auth=auth)
        else:
            print "\tThere are no more tweets to collect"
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
        if (limit is not None) and (len(entities) > limit):
            return set(list(entities)[0:limit])
    return entities


version = '1.0'
# End of pyTweet.py
