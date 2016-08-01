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
Authors: Kelly Geyer, Nicholas Stanisha
Date: October 29, 2015
Installation: Python 2.7 on Windows 7

Description: This script provides wrapper functions for the Official Twitter API. These functions enable collection of
profiles, timelines, friends and followers. During collection, the code is automatically pause for the rate limit.
"""

import pyTweet, requests, time, datetime, ujson, os, re, urllib, json, math, urllib2, uuid, random
from requests_oauthlib import OAuth1
from datetime import date, timedelta
import numpy as np

##
# MISCELANEOUS FUNCTIONS
def clear_place_savers(user_dir):
    """
    This function deletes the space saving parameters.

    :param user_dir: Directory to store place saver files
    """
    files_to_clear = ['place_saver_v1.txt', 'place_saver_v2.txt']
    for ff in files_to_clear:
        if os.path.isfile(os.path.join(user_dir, ff)):
            os.remove(os.path.join(user_dir, ff))

def download_tweet_media(link, proxies, save_dir=os.getcwd()):
    """
    This function downloads a single link (or list) of Twitter media. The media is saved in save_dir, and must have one
    of the following extensions: 'gif', 'jpg', 'jpeg', 'jif', 'jfif', 'tif', 'tiff', 'png', 'pdf', 'mp4'

    :param link: A single link string, or list of link strings
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param save_dir: Directory to save media files, default is current directory. Created if it doesn't exsit
    :return link_2_file_index: Dictionary correlating links with file names of downloaded media
    """
    # Check if directory exists, create it if doesn't
    if not os.path.isdir(save_dir):
        os.mkdir(save_dir)
    # Link to filename index
    link_2_file_index = {}
    # Handle extensions and URL object
    known_extensions = ['gif', 'jpg', 'jpeg', 'jif', 'jfif', 'tif', 'tiff', 'png', 'pdf', 'mp4']
    p = urllib2.ProxyHandler(proxies)
    opener = urllib2.build_opener(p)
    urllib2.install_opener(opener)
    if isinstance(link, str) or isinstance(link, unicode):
        link = [str(link)]
    for ll in link:
        # Check that the link's extentions matches one of those in known_extensions
        if sum([ll.endswith('.' + x) for x in known_extensions]) < 1:
            print "The link {} does not a have proper ending for an image or video".format(ll)
            continue
        # Get media extension
        spt_ll = ll.split('.')
        extension = spt_ll[len(spt_ll)-1]
        # Create filename for media
        fn = os.path.join(save_dir, "{}_file_{}.{}".format(extension, str(uuid.uuid4()), extension))
        # Save data
        try:
            f = open(fn, 'wb')
            f.write(urllib2.urlopen(ll).read())
            f.close()
            # Update link-to-filename index
            link_2_file_index[ll] = fn
        except urllib2.HTTPError:
            f.close()
            # Update link-to-filename index
            link_2_file_index[ll] = 'No file recovered from link'
            continue
    return link_2_file_index


##
# TWITTER AUTHORIZATION
def _get_twitter_certificate():
    """
    This function gets the location of Twitter API Certificate if it is stored in pyTweet's directory. If the .cer file
    cannot be found it will be created

    :return cafile: Filename of Twitter API certificate, including it's path
    """
    cafile = os.path.join(os.path.dirname(pyTweet.__file__), 'api.twitter.cer')     # Twitter API CA Certificate
    cert_url = 'http://curl.haxx.se/ca/cacert.pem'                                  # CA certificate site
    if not os.path.isfile(cafile):
        print "\tThe file {} cannot be found...downloading it from {}.".format(cafile, cert_url)
        file = open(cafile, 'wb')
        file.write(requests.get(cert_url).content)
        file.close()
    return cafile

def _select_random_key():
    """
    This file selects a random key JSON

    :return twitter_key: Dictionary object containing 'API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET'
    """
    key_jsons = _get_key_list()
    key = {}
    for k in key_jsons:
        try:
            key = ujson.load(open(k))
        except ValueError:
            continue
        if ('API_KEY' in key.keys()) and ('API_SECRET' in key.keys()) and ('ACCESS_TOKEN' in key.keys()) and ('ACCESS_TOKEN_SECRET' in key.keys()):
            key['KEY_FILE'] = k
            break
        else:
            print "\tWarning! The file {} does not contain a valid Twitter API key. Please refer to the documentation " \
                  "on creating an API key".format(key_jsons[k])
            continue
    assert (key != {}), "None of the Twitter key JSONs were formatted properly - refer to the documentation on " \
                        "creating key files. Your keys should be stored in the directory {}".format(os.path.join(os.path.dirname(pyTweet.__file__), 'twitter_api_keys'))
    return key

def load_twitter_api_key_set(key_file=''):
    """
    This funcitons loads a set of Twitter keys.

    :return twitter_keys: Dictionary object containing 'API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET'
    """
    if key_file != '':
        assert (os.path.isfile(key_file)), "The file {} does not appear to exist".format(key_file)
        # Open JSON file
        try:
            with open(key_file) as file_stream:
                key = json.load(file_stream)
        except ValueError:
            print "\tWarning! The file {} produces a value error"
            key = _select_random_key()
        if ('API_KEY' in key.keys()) and ('API_SECRET' in key.keys()) and ('ACCESS_TOKEN' in key.keys()) and ('ACCESS_TOKEN_SECRET' in key.keys()):
            key['KEY_FILE'] = key_file
        else:
            print "\tWarning! The file {} does not contain a valid Twitter API key. Please refer to the documentation " \
                  "on creating an API key".format(key_file)
    else:
        key = _select_random_key()
    return key

def get_authorization(twitter_keys):
    """
    This function obtains an authorization object for accessing the Official Twitter API.

    :param twitter_keys: Dictionary object containing 'API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET'
    :return OAUTH: Authorization object requred for remaining pyTweet collection functions
    """
    for tk in ['API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET', 'KEY_FILE']:
        assert (tk in twitter_keys.keys()), "The field '{}' has not been found and is required for authentication.".format(tk)
    oauth1 = OAuth1(client_key=twitter_keys['API_KEY'], client_secret=twitter_keys['API_SECRET'],
                    resource_owner_key=twitter_keys['ACCESS_TOKEN'],
                    resource_owner_secret=twitter_keys['ACCESS_TOKEN_SECRET'])
    OAUTH = {'API_KEY': twitter_keys['API_KEY'],
             'API_SECRET': twitter_keys['API_SECRET'],
             'ACCESS_TOKEN': twitter_keys['ACCESS_TOKEN'],
             'ACCESS_TOKEN_SECRET': twitter_keys['ACCESS_TOKEN_SECRET'],
             'KEY_FILE': twitter_keys['KEY_FILE'],
             'OAUTH': oauth1}
    return OAUTH

def check_twitter_key_functionality(host, port):
    """
    This function checks your saved Twitter API keys to ensure that they are functional. A message appears indicating
    each key's status.

    :param host: proxy host
    :param port: proxy port
    """
    # Find key files
    key_dir = os.path.join(os.path.dirname(pyTweet.__file__), 'twitter_api_keys')
    print "\nTest Twitter API keys saved in the directory {}.".format(key_dir)
    key_jsons = _get_key_list()
    assert (len(key_jsons) > 0), "You have no Twitter API key files saved in {}. Refer to the documentation to create " \
                                 "key files.".format(key_dir)
    # Get Twitter certificate
    cafile = _get_twitter_certificate()
    proxies = {'http': 'http://%s:%s' % (host, port), 'https': 'http://%s:%s' % (host, port)}
    good_key = None
    for kk in key_jsons:
        key = load_twitter_api_key_set(key_file=kk)
        # Be sure the key is formatted correctly
        if ('API_KEY' not in key.keys()) or ('API_SECRET' not in key.keys()) or ('ACCESS_TOKEN' not in key.keys()) or ('ACCESS_TOKEN_SECRET' not in key.keys()):
            print "\tNOT A VALID KEY: The file contains incorrectly formated key. Please refer to the documentation on " \
                  "creating an API key"
            continue
        print "CHECK TWITTER KEY FILE {}".format(kk)
        # Authorization with key
        oauth = get_authorization(key)
        url = 'https://api.twitter.com/1.1/users/suggestions.json'
        r = requests.get(url=url, proxies=proxies, auth=oauth['OAUTH'], verify=cafile)
        results = r.json()
        if (not isinstance(results, list)) and ('errors' in results.keys()):
            print "\tERROR CODE {}: {}".format(results['errors'][0]['code'], results['errors'][0]['message'])
            continue
        else:
            print "\tSUCCESSFUL AUTHENTICATION"
            good_key = kk
    assert (good_key is not None), "No valid keys were found in the directory {}".format(key_dir)

def _get_key_list():
    """
    This function returns the list of key files available.

    :return key_jsons: List of key JSON files
    """
    key_dir = os.path.join(os.path.dirname(pyTweet.__file__), 'twitter_api_keys')
    key_jsons = filter(lambda k: re.match('.+\.json', k), os.listdir(os.path.join(key_dir)))
    key_jsons = [os.path.join(key_dir, j) for j in key_jsons]
    random.shuffle(key_jsons)
    return key_jsons


##
# FUNCTIONALITY FOR CHECKING THE RATE LIMIT STATUS AND PAUSING AS NEEDED
def _rls_type_list():
    """
    Returns list of potential RLS type lables
    """
    return ['timeline', 'friends', 'followers', 'search_tweets', 'search_users', 'retweets', 'rls', 'users',
            'friends_list', 'followers_list', 'single_tweet', 'users_search', 'geo_reverse', 'place_id_lookup',
            'available_trends', '/trends/place', 'statuses/lookup']

def _find_best_twitter_key(type, reset, remaining, limit, proxies, auth):
    """
    This function switches to another pair of Twitter API keys, if they are available, to avoid pausing.

    * WANT TO SWAP KEYS HERE B/C PAUSE IS MORE THAN 3 MINUTES

    :param type: Type of API call: "timeline", "friends", "followers", "search_tweets", "search_users", "retweets",
                "rls", or "users"
    :param reset: The remaining window before the limit resets in UTC epoch seconds
    :param remaining: The number of requests left for the 15 minute window
    :param limit: The rate limit ceiling for that given reque
    :param proxies: Proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return best_key_auth: Authorization object using the best keys
    :return isNewAuth: Boolean value representing whether a new authorization has been produced
    """
    rls_types = _rls_type_list()
    assert (type in rls_types), "Specify an RLS type as: {}".format("', '".join(rls_types))
    # Count JSON files in key directory
    key_dir = os.path.join(os.path.dirname(pyTweet.__file__), 'twitter_api_keys')
    key_jsons = _get_key_list()
    isNewAuth = False
    # Check if there are enough keys to continue with this function
    assert (len(key_jsons) > 0), "You have no Twitter API key files saved in {}. \nRefer to the documentation to " \
                                 "create key files, or move your key files to that location.".format(key_dir)
    if len(key_jsons) == 1:
        print "\tThere are no other API keys to use...returning current API key."
        pause = abs(int(time.time()) - reset) + 5
        print "\tThere are no alternative keys. Pause for {} seconds.".format(pause)
        time.sleep(pause)
        return (auth, isNewAuth)
    # Define best auth and key
    best_key_auth = auth
    best_key = {}
    best_key[type] = {'RESET': reset, 'LIMIT': limit, 'REMAINING': remaining}

    for k in key_jsons:
        try:
            key = load_twitter_api_key_set(key_file=k)
        except (ValueError, AttributeError):
            print "\tWarning! The file {} does not contain a valid Twitter API key. Please refer to the " \
                  "documentation on creating an API key".format(k)
            continue
        if ('API_KEY' not in key.keys()) or ('API_SECRET' not in key.keys()) or ('ACCESS_TOKEN' not in key.keys()) or ('ACCESS_TOKEN_SECRET' not in key.keys()):
            print "\tWarning! The file {} does not contain a valid Twitter API key. Please refer to the documentation " \
                  "on creating an API key".format(k)
            continue
        # Be sure that this is not the same key we started the function with
        if auth['KEY_FILE'] == k:
            continue
        if (auth['API_KEY'] == key['API_KEY']) and (auth['API_SECRET'] == key['API_SECRET']) and (auth['ACCESS_TOKEN'] == key['ACCESS_TOKEN']) and (auth['ACCESS_TOKEN_SECRET'] == key['ACCESS_TOKEN_SECRET']):
            continue

        # Check the RLS of RLS for key
        key_auth = get_authorization(key)
        _, _, _ = _get_rate_limit_status(type=type, proxies=proxies, auth=key_auth)
        key = load_twitter_api_key_set(key_file=k)
        # Skip key if it doesn't have appropriate fields
        if ('RESET' not in key[type].keys()) or ('REMAINING' not in key[type].keys()) or ('LIMIT' not in key[type].keys()):
            continue

        # Check keys!
        if key[type]['REMAINING'] == key[type]['LIMIT']:
            best_key = key
            best_key_auth = key_auth
            isNewAuth = True
            break
        if key[type]['REMAINING'] < 1:
            continue
        if key[type]['REMAINING'] > best_key[type]['REMAINING']:
            best_key = key
            best_key_auth = key_auth
            isNewAuth = True
            break

    if isNewAuth:
        print "\nSwitch to Twitter key {} after using {}".format(best_key_auth['KEY_FILE'], auth['KEY_FILE'])
    else:
        pause = abs(int(time.time()) - best_key[type]['RESET']) + 5
        print "\nUnable to find a better Twitter key, they all appear to be exahusted for the {} call. \nPause for {} " \
              "minutes".format(type, np.ceil(pause/60))
        time.sleep(pause)
    return (best_key_auth, isNewAuth)

def _get_rate_limit_status(type, proxies, auth):
    """
    This function returns the call limit, remaining calls, and reset time.

    :param type: Type of API call: "timeline", "friends", "followers", "retweets", "rls", or "users"
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return reset: The remaining window before the limit resets in UTC epoch seconds
    :return remaining: the number of requests left for the 15 minute window
    :return limit: the rate limit ceiling for that given request
    """
    rls_types = _rls_type_list()
    assert (type in rls_types), "Specify an RLS type as: {}".format("', '".join(rls_types))
    assert (len(_get_key_list()) > 0), "You have no keys!"
    # Load Twitter key and fill in missing fields
    api_key = load_twitter_api_key_set(key_file=auth['KEY_FILE'])
    if type not in api_key.keys():
        api_key[type] = {}
        ujson.dump(api_key, open(auth['KEY_FILE'], 'w'))
    # Update limit, reset, and remaining values
    if ('LIMIT' in api_key[type].keys()) and ('RESET' in api_key[type].keys()) and ('REMAINING' in api_key[type].keys()):
        if api_key[type]['RESET'] < time.time():
            api_key[type]['RESET'] = time.time() + 900
            api_key[type]['REMAINING'] = api_key[type]['LIMIT']
            ujson.dump(api_key, open(auth['KEY_FILE'], 'w'))
        return (api_key[type]['RESET'], api_key[type]['REMAINING'], api_key[type]['LIMIT'])
    # Check RLS for rate limit status
    if 'RLS_CALLS' in api_key.keys():
        window_bound = time.time() - 900
        api_key['RLS_CALLS'] = [i for i in api_key['RLS_CALLS'] if i > window_bound]
        ujson.dump(api_key, open(auth['KEY_FILE'], 'w'))
        if len(api_key['RLS_CALLS']) >= 180:
            return (time.time() + 900, 0, 180)
    else:
        api_key['RLS_CALLS'] = []
    # Check RLS for any other rate
    url = 'https://api.twitter.com/1.1/application/rate_limit_status.json?resources='
    cafile = _get_twitter_certificate()
    api_call_lookup = {'timeline': 'statuses', 'friends': 'friends', 'friends_list': 'friends',
                       'followers': 'followers', 'followers_list': 'followers', 'users': 'users',
                       'retweets': 'statuses', 'search_tweets': 'search', 'single_tweet': 'statuses',
                       'users_search': 'users', 'geo_reverse': 'geo', 'place_id_lookup': 'geo',
                       'available_trends': 'trends', '/trends/place': 'trends', 'statuses/lookup': 'statuses'}
    try:
        rateLimitStatus = requests.get(url=url+api_call_lookup[type], proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        api_key['RLS_CALLS'].append(time.time())
        ujson.dump(api_key, open(auth['KEY_FILE'], 'w'))
    except requests.exceptions.ConnectionError:
        print "Exceeded rate limit of RLS calls. Must get reset, remaining, and limit for '{}' rate limit status.".format(type)
        return (time.time() + 900, 0, 180)
    rls_json = rateLimitStatus.json()
    # Check for RLS Error
    if ('error' in rls_json.keys()) or ('errors' in rls_json.keys()):
        print "\nERROR CODE {}: {} for the key file {}".format(rls_json['errors'][0]['code'], rls_json['errors'][0]['message'], auth['KEY_FILE'])
        return (time.time()+900, 0, 180)
    # Check RLS for any other rate
    if type == 'statuses/lookup':
        reset = rls_json['resources']['statuses']['/statuses/lookup']['reset']
        remaining = rls_json['resources']['statuses']['/statuses/lookup']['remaining']
        limit = rls_json['resources']['statuses']['/statuses/lookup']['limit']
    if type == 'timeline':
        reset = rls_json['resources']['statuses']['/statuses/user_timeline']['reset']
        remaining = rls_json['resources']['statuses']['/statuses/user_timeline']['remaining']
        limit = rls_json['resources']['statuses']['/statuses/user_timeline']['limit']
    if type == 'friends':
        reset = rls_json['resources']['friends']['/friends/ids']['reset']
        remaining = rls_json['resources']['friends']['/friends/ids']['remaining']
        limit = rls_json['resources']['friends']['/friends/ids']['limit']
    if type == 'followers':
        reset = rls_json['resources']['followers']['/followers/ids']['reset']
        remaining = rls_json['resources']['followers']['/followers/ids']['remaining']
        limit = rls_json['resources']['followers']['/followers/ids']['limit']
    if type == 'users':
        reset = rls_json['resources']['users']['/users/lookup']['reset']
        remaining = rls_json['resources']['users']['/users/lookup']['remaining']
        limit = rls_json['resources']['users']['/users/lookup']['limit']
    if type == 'retweets':
        reset = rls_json['resources']['statuses']['/statuses/retweets/:id']['reset']
        remaining = rls_json['resources']['statuses']['/statuses/retweets/:id']['remaining']
        limit = rls_json['resources']['statuses']['/statuses/retweets/:id']['limit']
    if (type == 'search_tweets') or (type == 'search_users'):
        reset = rls_json['resources']['search']['/search/tweets']['reset']
        remaining = rls_json['resources']['search']['/search/tweets']['remaining']
        limit = rls_json['resources']['search']['/search/tweets']['limit']
    # if type == 'search_users':
    #     reset = rls_json['resources']['search']['/search/tweets']['reset']
    #     remaining = rls_json['resources']['search']['/search/tweets']['remaining']
    #     limit = rls_json['resources']['search']['/search/tweets']['limit']
    if type == 'friends_list':
        reset = rls_json['resources']['friends']['/friends/list']['reset']
        remaining = rls_json['resources']['friends']['/friends/list']['remaining']
        limit = rls_json['resources']['friends']['/friends/list']['limit']
    if type == 'followers_list':
        reset = rls_json['resources']['followers']['/followers/list']['reset']
        remaining = rls_json['resources']['followers']['/followers/list']['remaining']
        limit = rls_json['resources']['followers']['/followers/list']['limit']
    if type == 'single_tweet':
        reset = rls_json['resources']['statuses']['/statuses/show/:id']['reset']
        remaining = rls_json['resources']['statuses']['/statuses/show/:id']['remaining']
        limit = rls_json['resources']['statuses']['/statuses/show/:id']['limit']
    if type == 'users_search':
        reset = rls_json['resources']['users']['/users/search']['reset']
        remaining = rls_json['resources']['users']['/users/search']['remaining']
        limit = rls_json['resources']['users']['/users/search']['limit']
    if type == 'geo_reverse':
        reset = rls_json['resources']['geo']['/geo/reverse_geocode']['reset']
        remaining = rls_json['resources']['geo']['/geo/reverse_geocode']['remaining']
        limit = rls_json['resources']['geo']['/geo/reverse_geocode']['limit']
    if type == 'place_id_lookup':
        reset = rls_json['resources']['geo']['/geo/id/:place_id']['reset']
        remaining = rls_json['resources']['geo']['/geo/id/:place_id']['remaining']
        limit = rls_json['resources']['geo']['/geo/id/:place_id']['limit']
    if type == 'available_trends':
        reset = rls_json['resources']['trends']['/trends/available']['reset']
        remaining = rls_json['resources']['trends']['/trends/available']['remaining']
        limit = rls_json['resources']['trends']['/trends/available']['limit']
    if type == '/trends/place':
        reset = rls_json['resources']['trends']['/trends/place']['reset']
        remaining = rls_json['resources']['trends']['/trends/place']['remaining']
        limit = rls_json['resources']['trends']['/trends/place']['limit']
    api_key[type]['RESET'] = reset
    api_key[type]['REMAINING'] = remaining
    api_key[type]['LIMIT'] = limit
    api_key['RLS_CALLS'].append(time.time())
    ujson.dump(api_key, open(auth['KEY_FILE'], 'w'))
    return (reset, remaining, limit)

def _swap_keys(type, proxies, auth):
    """
    This function swaps keys in the case of an EOF error.

    :param type: type of call: "timeline", "friends", "followers", "search_tweets", "search_users", "retweets", or "users"
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    """
    # Check parameters
    rls_types = _rls_type_list()
    assert (type in rls_types), "Specify an RLS type as: {}".format("', '".join(rls_types))
    # Collect reset and remaining
    (reset, remaining, limit) = _get_rate_limit_status(type=type, proxies=proxies, auth=auth)
    (auth, isNewAuth) = _find_best_twitter_key(type=type, reset=reset, remaining=remaining, limit=limit, proxies=proxies, auth=auth)
    if not isNewAuth:
        pause = abs(int(time.time()) - reset) + 5
        print "There isn't a better key to use"

        # Pause if needed
    if remaining < 1:
        pause = abs(int(time.time()) - reset) + 5
        # If pause is longer than 3 minutes try another set of keys...
        if pause > 300:
            # print "SWITCHING TWITTER KEYS"
            (auth, isNewAuth) = _find_best_twitter_key(type=type, reset=reset, remaining=remaining, limit=limit, proxies=proxies, auth=auth)
        else:
            print "There is less than one call remaining in this window. Pause for {} seconds.".format(math.ceil(pause))
            time.sleep(pause)
    return auth

def _check_rate_limit_status(type, auth, proxies):
    """
    This function checks the rate limit for an API call and pauses as specified by the API

    :param type: type of call: "timeline", "friends", "followers", "search_tweets", "search_users", "retweets", or "users"
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return auth: New, or same, Twitter application authentication, see the get_authorization method
    """
    # Check parameters
    rls_types = _rls_type_list()
    assert (type in rls_types), "Specify an RLS type as: {}".format("', '".join(rls_types))
    # Collect reset and remaining
    (reset, remaining, limit) = _get_rate_limit_status(type=type, proxies=proxies, auth=auth)
    # Pause if needed
    if remaining < 1:
        pause = abs(int(time.time()) - reset) + 5
        # If pause is longer than 3 minutes try another set of keys...
        if pause > 300:
            # print "SWITCHING TWITTER KEYS"
            (auth, isNewAuth) = _find_best_twitter_key(type=type, reset=reset, remaining=remaining, limit=limit, proxies=proxies, auth=auth)
        else:
            print "There is less than one call remaining in this window. Pause for {} seconds.".format(math.ceil(pause))
            time.sleep(pause)
    return auth

def _discount_remaining_calls(type, proxies, auth):
    """
    This function discounts the remaining 'type' after an API call

    :param type: Type of API call: "timeline", "friends", "followers", "retweets", "rls", or "users"
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Authentication object
    """
    rls_types = _rls_type_list()
    assert (type in rls_types), "Specify an RLS type as: {}".format("', '".join(rls_types))
    assert (os.path.isfile(auth['KEY_FILE'])), "The file {} does not appear to exist".format(auth['KEY_FILE'])
    # Load current key
    current_key = load_twitter_api_key_set(key_file=auth['KEY_FILE'])
    # Make sure that key has all of the relevant fields, create them otherwise
    if type not in current_key.keys():
        current_key[type] = {}
    need_fields = ['LIMIT', 'REMAINING', 'RESET']
    for ff in need_fields:
        if ff not in current_key[type].keys():
            (reset, remaining, limit) = _get_rate_limit_status(type=type, proxies=proxies, auth=auth)
            current_key[type]['LIMIT'] = limit
            current_key[type]['REMAINING'] = remaining
            current_key[type]['RESET'] = reset
            break
    current_key[type]['REMAINING'] -= 1
    ujson.dump(current_key, open(auth['KEY_FILE'], 'w'))


##
# FUNCTIONS FOR COLLECTING PROFILE INFORMATION
def user_lookup_usernames(user_list, proxies, auth, include_entities=''):
    """
    Look up user information for a list of usernames. If a user's account has been deleted then it will not be returned
    in the .json of user information.

    :param user_list: Twitter user name, or list of user names
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param include_entities: The entities node that may appear within embedded statuses will be disincluded when set to false.
    :return user_info: list of JSON user objects that could be returned.
    """
    # Check parameters
    if (user_list is None) or (user_list == []): return []
    if not isinstance(user_list, list):
        user_list = [user_list]
    params = {}
    if include_entities != '':
        assert (isinstance(include_entities, bool)), "The parameter 'include_entities' must be a boolean"
        params['include_entities'] = include_entities
    # Split usernames into groups of 100
    usernames = [user_list[i:i+100] for i in range(0, len(user_list), 100)]
    # print "split usernames: ", usernames
    # Prepare API call
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='users', proxies=proxies, auth=auth)     # Check RLS
    url = 'https://api.twitter.com/1.1/users/lookup.json?screen_name=' + ','.join(usernames[0])
    if params != {}:
        url += '&' + urllib.urlencode(params)
    # print "url: ", url
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='users', proxies=proxies, auth=auth)
    user_info = r.json()
    # Additional requests for user information, AKA more than 100 user names requested
    if len(usernames) > 1:
        for i in range(1, len(usernames)):
            auth = _check_rate_limit_status(type='users', proxies=proxies, auth=auth)
            url = 'https://api.twitter.com/1.1/users/lookup.json?screen_name=' + ','.join(usernames[0])
            if params != {}:
                url += '&' + urllib.urlencode(params)
            # print "url: ", url
            r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
            _discount_remaining_calls(type='users', proxies=proxies, auth=auth)
            user_info = user_info + r.json()
    return user_info

def user_lookup_userids(user_list, proxies, auth, include_entities=''):
    """
    Look up user information for a list of user IDs. If a user's account has been deleted then it will not be returned
    in the .json of user information.

    :param user_list: list of user ids
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param include_entities: The entities node that may appear within embedded statuses will be disincluded when set to false.
    :return user_info: JSON list of user information that could be retrieved
    """
    # Check parameters
    if (user_list is None) or (len(user_list) < 1): return []
    if not isinstance(user_list, list):
        user_list = [user_list]
    params = {}
    if include_entities != '':
        assert (isinstance(include_entities, bool)), "The parameter 'include_entities' must be a boolean"
        params['include_entities'] = include_entities
    # Split user IDs into groups of 100
    userids = [user_list[i:i+100] for i in range(0, len(user_list), 100)]
    # Prepare API request
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='users', proxies=proxies, auth=auth)       # RLS check
    url = 'https://api.twitter.com/1.1/users/lookup.json?user_id=' + ','.join([str(j) for j in userids[0]])
    if params != {}:
        url += '&' + urllib.urlencode(params)
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='users', proxies=proxies, auth=auth)
    try:
        user_info = r.json()
    except ValueError:
        print "ValueError: No JSON object could be decoded"
        return []
    # Additional requests for user information, AKA more than 100 user IDs requested
    if len(userids) > 1:
        for i in range(len(userids)):
            auth = _check_rate_limit_status(type='users', proxies=proxies, auth=auth)
            url = 'https://api.twitter.com/1.1/users/lookup.json?user_id=' + ','.join([str(j) for j in userids[i]])
            if params != {}:
                url += '&' + urllib.urlencode(params)
            r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
            _discount_remaining_calls(type='users', proxies=proxies, auth=auth)
            if 'errors' in r.json():
                continue
            user_info = user_info + r.json()
    return user_info

def lookup_users(proxies, auth, screen_names='', user_ids='', include_entities=''):
    """
    Returns a list of user dictionaries, as specified by comma-separated values passed to the user_id and/or screen_name
    parameters. This method is especially useful when used in conjunction with collections of user IDs returned from
    get_user_friends and get_user_followers.

    There are a few things to note when using this method.
    * You must be following a protected user to be able to see their most recent status update. If you don' follow a
        protected user their status will be removed.
    * The order of user IDs or screen names may not match the order of users in the returned array.
    * If a requested user is unknown, suspended, or deleted, then that user will not be returned in the results list.
    * If none of your lookup criteria can be satisfied by returning a user object, a HTTP 404 will be thrown.

    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param screen_names: List of screen names, or a single one - optional
    :param user_ids: List of user IDs, or a single one - optional
    :param include_entities: The entities node that may appear within embedded statuses will be disincluded when set to false.
    :return user_info: JSON list of user information that could be retrieved
    """
    # Check parameters
    params = {}
    assert ((user_ids != '') or (screen_names != '')), "You must specify a values for at least 'user_ids' or 'screen_names'"
    if user_ids != '':
        if not isinstance(user_ids, list):
            user_ids = [user_ids]
        user_ids = [str(ii) for ii in user_ids]
    else:
        user_ids = []
    if screen_names != '':
        if not isinstance(screen_names, list):
            screen_names = [screen_names]
    else:
        screen_names = []
    if include_entities != '':
        assert (isinstance(include_entities, bool)), "The parameter 'include_user_entities' must be boolean"
        params['include_entities'] = include_entities
    # Prepare for collection
    cafile = _get_twitter_certificate()     # Get Twitter certificate
    user_info = []
    # Collect user IDs
    if len(user_ids) > 0:
        # Split user IDs and screen names into groups of 100
        USERIDS = [user_ids[i:i+100] for i in range(0, len(user_ids), 100)]
        for ids in USERIDS:
            # Prepare API request
            auth = _check_rate_limit_status(type='users', proxies=proxies, auth=auth)       # RLS check
            url = 'https://api.twitter.com/1.1/users/lookup.json?user_id=' + ','.join([str(j) for j in ids])
            if params != {}:
                url += '&' + urllib.urlencode(params)
            r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
            _discount_remaining_calls(type='users', proxies=proxies, auth=auth)
            try:
                if 'errors' in r.json():
                    continue
            except ValueError:
                continue
            user_info += r.json()
    # Collect screen names
    if len(screen_names) > 0:
        # Split user IDs and screen names into groups of 100
        SCREENNAMES = [screen_names[i:i+100] for i in range(0, len(screen_names), 100)]
        for sn in SCREENNAMES:
            auth = _check_rate_limit_status(type='users', proxies=proxies, auth=auth)       # RLS check
            url = 'https://api.twitter.com/1.1/users/lookup.json?user_id=' + ','.join([str(j) for j in sn])
            if params != {}:
                url += '&' + urllib.urlencode(params)
            r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
            _discount_remaining_calls(type='users', proxies=proxies, auth=auth)
            try:
                if 'errors' in r.json():
                    continue
            except ValueError:
                continue
            user_info += r.json()
    return user_info

def get_user_friends(proxies, auth, limit=None, user_id='', screen_name=''):
    """
    Look up the IDs of all of a user's friends (people they follow), and return them in a list.

    :param user_id: The ID of the user for whom to return results for - optional
    :param screen_name: The screen name of the user for whom to return results for - optional
    :param limit: limit to number of friends to collect. Set to None to get all friends. this is the default
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return friends_list: list of user's friends' IDs
    """
    # Check parameters
    params = {'count': '5000'}
    assert ((user_id != '') or (screen_name != '')), "You must specify a value for either 'user_id' or 'screen_name'"
    if user_id != '':
        params['user_id'] = str(user_id)
    if screen_name != '':
        params['screen_name'] = screen_name
    if ('user_id' in params.keys()) and ('screen_name' in params.keys()):
        print "Only needs 'user_id' or 'screen_name', selecting 'user_id' for friend list query..."
        params.pop('screen_name')
    # Set API calls based on limit
    if (limit is not None) and (int(limit) <= 5000):
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Prepare API call
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='friends', proxies=proxies, auth=auth)      # Check RLS
    params['cursor'] = '-1'
    url = 'https://api.twitter.com/1.1/friends/ids.json?' + urllib.urlencode(params)
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='friends', proxies=proxies, auth=auth)
    r2 = r.json()
    friends_list = []
    if ('errors' in r2.keys()) or ('error' in r2.keys()):
        print "Error: ", r2
        return friends_list
    try:
        friends_list = r2['ids']
        next_cursor = r2['next_cursor']
    except KeyError:
        print "\tKey Error in collecting friends."
        return friends_list
    # Are there more friends?
    while keepLooking and (next_cursor != 0):
        print "\tCollect more friends..."
        # Prepare API call
        auth = _check_rate_limit_status(type='friends', proxies=proxies, auth=auth)     # Check RLS
        params['cursor'] = str(next_cursor)
        url = 'https://api.twitter.com/1.1/friends/ids.json?' + urllib.urlencode(params)
        r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        _discount_remaining_calls(type='friends', proxies=proxies, auth=auth)
        r2 = r.json()
        if ('errors' in r2.keys()) or ('error' in r2.keys()):
            print "Error: ", r2
            return friends_list
        friends_list += r2['ids']
        next_cursor = int(r2['next_cursor'])
        if limit <= len(friends_list):
            keepLooking = False
    # Remove extra friends from the list
    if (limit is not None) and (len(friends_list) > limit):
        friends_list = friends_list[0:limit]
    return friends_list

def get_user_friend_profiles(proxies, auth, limit=None, user_id='', screen_name='', skip_status='', include_user_entities=''):
    """
    Returns a list of user dictionaries for every user the specified user is following (otherwise known as their
    'friends'). Either either user ID or screen name, if both are specified user ID is used

    :param user_id: Unique Twitter user ID, optional
    :param screen_name: Twitter screen name, optional
    :param limit: limit to number of friends to collect. Set to None to get all friends. this is the default
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param skip_status: When set to either true, t or 1 statuses will not be included in the returned user objects
    :param include_user_entities: The user object entities node will be disincluded when set to false.
    :return friends_list: list of user's friends' profile dictionary
    """
    # Check parameters
    params = {'count': '200'}
    assert ((user_id != '') or (screen_name != '')), "You must specify a value for either 'user_id' or 'screen_name'"
    if user_id != '':
        params['user_id'] = str(user_id)
    if screen_name != '':
        params['screen_name'] = screen_name
    if skip_status != '':
        assert (isinstance(skip_status, bool)), "The parameter 'skip_status' must be boolean"
        params['skip_status'] = skip_status
    if include_user_entities != '':
        assert (isinstance(include_user_entities, bool)), "The parameter 'include_user_entities' must be boolean"
        params['include_user_entities'] = include_user_entities
    if ('user_id' in params.keys()) and ('screen_name' in params.keys()):
        print "Only needs 'user_id' or 'screen_name', selecting 'user_id' for friend list query..."
        params.pop('screen_name')
    # Set API calls based on limit
    if (limit is not None) and (int(limit) <= 20):
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Prepare API call to get friends
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='friends_list', proxies=proxies, auth=auth)        # Check RLS
    params['cursor'] = '-1'
    url = 'https://api.twitter.com/1.1/friends/list.json?' + urllib.urlencode(params)
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='friends_list', proxies=proxies, auth=auth)
    r2 = r.json()
    friends_list = []
    if ('errors' in r2.keys()) or ('error' in r2.keys()):
        print "Error: ", r2
        return friends_list
    try:
        friends_list = r2['users']
        next_cursor = r2['next_cursor']
    except KeyError:
        print "\tKey Error in collecting friends."
        return friends_list
    # Are there more friends?
    while keepLooking and (next_cursor != 0):
        print "\tCollect more friends..."
        # Prepare API call to get friends
        auth = _check_rate_limit_status(type='friends_list', proxies=proxies, auth=auth)        # Check RLS
        params['cursor'] = str(next_cursor)
        url = 'https://api.twitter.com/1.1/friends/list.json?' + urllib.urlencode(params)
        r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        _discount_remaining_calls(type='friends_list', proxies=proxies, auth=auth)
        r2 = r.json()
        if ('errors' in r2.keys()) or ('error' in r2.keys()):
            print "Error: ", r2
            return friends_list
        friends_list += r2['users']
        next_cursor = int(r2['next_cursor'])
        if limit <= len(friends_list):
            keepLooking = False
    # Remove extra friends from the list
    if (limit is not None) and (len(friends_list) > limit):
        friends_list = friends_list[0:limit]
    return friends_list

def get_user_followers(proxies, auth, limit=None, user_id='', screen_name=''):
    """
    Look up the IDs of all of a user's followers (people who follow them), and return them in a list of json objects.

    :param user_id: The ID of the user for whom to return results for - optional
    :param screen_name: The screen name of the user for whom to return results for - optional
    :param limit: limit to number of friends to collect. Set to None to get all friends - this is the default
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return followers_list: list of user's followers' IDs
    """
    # Check parameters
    params = {'count': '5000'}
    assert ((user_id != '') or (screen_name != '')), "You must specify a value for either 'user_id' or 'screen_name'"
    if user_id != '':
        params['user_id'] = str(user_id)
    if screen_name != '':
        params['screen_name'] = screen_name
    if ('user_id' in params.keys()) and ('screen_name' in params.keys()):
        print "Only needs 'user_id' or 'screen_name', selecting 'user_id' for friend list query..."
        params.pop('screen_name')
    # Set API calls based on limit
    if (limit is not None) and limit <= 5000:
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Format API call to get followers
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='followers', proxies=proxies, auth=auth)       # Check RLS
    params['cursor'] = '-1'
    url = 'https://api.twitter.com/1.1/followers/ids.json?' + urllib.urlencode(params)
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='followers', proxies=proxies, auth=auth)
    r2 = r.json()
    followers_list = []
    if ('errors' in r2.keys()) or ('error' in r2.keys()):
        print "Error: ", r2
        return followers_list
    try:
        followers_list = r2['ids']
        next_cursor = r2['next_cursor']
    except KeyError:
        print "Key Error in collecting followers."
        return followers_list
    # Are there more followers?
    while keepLooking and (next_cursor != 0):
        print "\tCollect more followers..."
        # Make API call
        auth = _check_rate_limit_status(type='followers', proxies=proxies, auth=auth)       # Check RLS
        params['cursor'] = str(next_cursor)
        url = 'https://api.twitter.com/1.1/followers/ids.json?' + urllib.urlencode(params)
        r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        _discount_remaining_calls(type='followers', proxies=proxies, auth=auth)
        r2 = r.json()
        if ('errors' in r2.keys()) or ('error' in r2.keys()):
            print "Error: ", r2
            return followers_list
        followers_list += r2['ids']
        # Is list complete? Has the limit been met, or does next_cursor equal 0?
        next_cursor = r2['next_cursor']
        if limit <= len(followers_list):
            keepLooking = False
    # Remove extra followers from list
    if (limit is not None) and (len(followers_list) > limit):
        followers_list = followers_list[0:limit]
    # Return follower list
    return followers_list

def get_user_follower_profiles(proxies, auth, limit=None, user_id='', screen_name='', skip_status='', include_user_entities=''):
    """
    Returns a list of user dictionaries for users following the specified user.

    :param user_id: The ID of the user for whom to return results for - optional
    :param screen_name: The screen name of the user for whom to return results for - optional
    :param limit: limit to number of friends to collect. Set to None to get all friends - this is the default
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param skip_status: When set to either true, t or 1 statuses will not be included in the returned user objects
    :param include_user_entities: The user object entities node will be disincluded when set to false.
    :return followers_list: list of user's followers' dictionaries
    """
    # Check parameters
    params = {'count': '200'}
    assert ((user_id != '') or (screen_name != '')), "You must specify a value for either 'user_id' or 'screen_name'"
    if user_id != '':
        params['user_id'] = str(user_id)
    if screen_name != '':
        params['screen_name'] = screen_name
    if skip_status != '':
        assert (isinstance(skip_status, bool)), "The parameter 'skip_status' must be boolean"
        params['skip_status'] = skip_status
    if include_user_entities != '':
        assert (isinstance(include_user_entities, bool)), "The parameter 'include_user_entities' must be boolean"
        params['include_user_entities'] = include_user_entities
    if ('user_id' in params.keys()) and ('screen_name' in params.keys()):
        print "Only needs 'user_id' or 'screen_name', selecting 'user_id' for friend list query..."
        params.pop('screen_name')
    # Set API calls based on limit
    if (limit is not None) and limit <= 20:
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Prepare API call to get followers
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='followers_list', proxies=proxies, auth=auth)        # Check RLS
    params['cursor'] = '-1'
    # url = 'https://api.twitter.com/1.1/friends/list.json?' + urllib.urlencode(params)
    url='https://api.twitter.com/1.1/followers/list.json?' + urllib.urlencode(params)
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='followers_list', proxies=proxies, auth=auth)
    r2 = r.json()
    followers_list = []
    if ('errors' in r2.keys()) or ('error' in r2.keys()):
        print "Error: ", r2
        return followers_list
    try:
        followers_list = r2['users']
        next_cursor = r2['next_cursor']
    except KeyError:
        print "Key Error in collecting followers."
        return followers_list
    # Are there more followers?
    while keepLooking and (next_cursor != 0):
        print "\tCollect more followers..."
        # Make API call
        auth = _check_rate_limit_status(type='followers_list', proxies=proxies, auth=auth)
        params['cursor'] = str(next_cursor)
        # url = 'https://api.twitter.com/1.1/friends/list.json?' + urllib.urlencode(params)
        url='https://api.twitter.com/1.1/followers/list.json?' + urllib.urlencode(params)
        r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        _discount_remaining_calls(type='followers_list', proxies=proxies, auth=auth)
        r2 = r.json()
        if ('errors' in r2.keys()) or ('error' in r2.keys()):
            print "Error: ", r2
            return followers_list
        followers_list += r2['users']
        # Is list complete? Has the limit been met, or does next_cursor equal 0?
        next_cursor = r2['next_cursor']
        if limit <= len(followers_list):
            keepLooking = False
    # Remove extra followers from list
    if (limit is not None) and (len(followers_list) > limit):
        followers_list = followers_list[0:limit]
    # Return follower list
    return followers_list


##
# FUNCTIONS FOR COLLECTING TIMELINE INFORMATION
def _convert_twitter_date(twitter_date):
    """
    Convert Twitter date

    :param twitter_date: date of creation in Twitter's format
    :return converted_twitter_date: Python date object
    """
    twitter_date = twitter_date.lstrip()
    q = str(twitter_date[4:11] + twitter_date[(len(twitter_date)-4):len(twitter_date)])
    converted_twitter_date = datetime.datetime.strptime(q, "%b %d %Y").date()
    return converted_twitter_date

def get_timeline(proxies, auth, start_date='', user_id='', screen_name='', trim_user='', exclude_replies='', contributor_details='', include_rts=''):
    """
    Find timeline of a user occuring after start_date. Enter either a screen name or user ID. User timelines belonging
    to protected users may only be requested when the authenticated user either 'owns' the timeline or is an approved
    follower of the owner. The timeline returned is the equivalent of the one seen when you view a user's profile on
    twitter.com. This method can only return up to 3,200 of a user's most recent Tweets. Native retweets of other
    statuses by the user is included in this total, regardless of whether include_rts is set to false when requesting
    this resource.

    :param user_id: The ID of the user for whom to return results for.
    :param screen_name: The screen name of the user for whom to return results for.
    :param trim_user: When set to true, each tweet returned in a timeline will include a user object including only the
            status authors numerical ID. Omit this parameter to receive the complete user object.
    :param exclude_replies: This boolean parameter will prevent replies from appearing in the returned timeline. Using
            exclude_replies will mean you will receive up-to count tweets
    :param contributor_details: This boolean parameter enhances the contributors element of the status response to
            include the screen_name of the contributor. By default only the user_id of the contributor is included.
    :param include_rts: When set to false, the timeline will strip any native retweets (though they will still count
            toward both the maximal length of the timeline and the slice selected by the count parameter). Note: If
            you're using the trim_user parameter in conjunction with include_rts, the retweets will still contain a full
            user object.
    :param start_date: start of timeline segment to collect, this is a datetime.date object. The default value is 52
            weeks ago from today
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return timeline: timeline dictionary of user, None if one doesn't exist
    """
    # Check parameters
    params = {'count': '200'}
    assert ((user_id != '') or (screen_name != '')), "You must set a value for either parameters 'user_id' or 'screen_name'"
    if start_date != '':
        assert (isinstance(start_date, datetime.date)), "The parameter 'start_date' must be a datetime.date object"
    else:
        one_year = datetime.timedelta(years=52)
        start_date = datetime.date.today() - one_year
    if user_id != '':
        params['user_id'] = str(user_id)
    if screen_name != '':
        params['screen_name'] = screen_name
    if trim_user != '':
        assert (isinstance(trim_user, bool)), "The parameter 'trim_user' must be a bool"
        params['trim_user'] = trim_user
    if exclude_replies != '':
        assert (isinstance(exclude_replies, bool)), "The parameter 'exclude_replies' must be a bool"
        params['exclude_replies'] = exclude_replies
    if contributor_details != '':
        assert (isinstance(contributor_details, bool)), "The parameter 'contributor_details' must be a bool"
        params['contributor_details'] = contributor_details
    if include_rts != '':
        assert (isinstance(include_rts, bool)), "The parameter 'include_rts' must be a bool"
        params['include_rts'] = include_rts
    if ('user_id' in params.keys()) and ('screen_name' in params.keys()):
        print "\nOnly need one of the parameters 'user_id' and 'screen_name'. Sticking with 'user_id' for timeline search"
        params.pop('screen_name')

    # Prepare for collection
    cafile = _get_twitter_certificate()
    # Get user for printing...
    if 'user_id' in params.keys():
        user = params['user_id']
    else:
        user = params['screen_name']

    # Initial collection
    url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?' + urllib.urlencode(params)
    auth = _check_rate_limit_status(type='timeline', proxies=proxies, auth=auth)
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='timeline', proxies=proxies, auth=auth)
    try:
        timeline = r.json()
    except ValueError:
        print ValueError
        print r
        return []
    # API Error (possibly due to privacy settings on user account)
    if isinstance(timeline, dict) and (('error' in timeline.keys()) or ('errors' in timeline.keys())):
        print "Timeline error: ", timeline
        return []
    # Check for empty timeline
    if len(timeline) < 1:
        print "\tThe timeline for user {} is empty".format(user)
        return []
    # Continue timeline collection, if specified...
    try:
        max_id = timeline[len(timeline)-1]['id']
        oldest_date = _convert_twitter_date(timeline[len(timeline)-1]['created_at'])
    except KeyError:
        print "KEY ERROR: skip this timeline"
        return []
    while (oldest_date - start_date).days > 0:
        print "\tCollect another page of tweets for the user {}".format(user)
        params['max_id'] = str(max_id)
        url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?' + urllib.urlencode(params)
        auth = _check_rate_limit_status(type='timeline', proxies=proxies, auth=auth)
        r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        _discount_remaining_calls(type='timeline', proxies=proxies, auth=auth)
        try:
            rr2 = r.json()
        except ValueError:
            print "\tThere are no more tweets to collect"
            break
        if isinstance(rr2, dict) and (('error' in rr2.keys()) or ('errors' in rr2.keys())):
            print "Timeline Error: ", rr2
            break
        if (rr2 != None) and (len(rr2) > 1):
            rr2.remove(rr2[0])      # Remove redundant tweet
            timeline += rr2         # Add new tweets to list
            # update new_date and max_id
            oldest_date = _convert_twitter_date(rr2[len(rr2)-1]['created_at'])
            max_id = rr2[len(rr2)-1]['id']
        else:
            print "\tThere are no more tweets to collect from user {}".format(user)
            break
    return timeline

def collect_user_timeline(USER, USER_type, start_date, proxies, auth):
    """
    Find timeline of a user occuring after start_date.

    :param USER: Can be either a Twitter user ID (numeric), Twitter user ID (string), or Twitter screen name.
    :param USER_type: specifies whether USER is an user ID or a screen name, enter either 'user_id' or 'screen_name'
    :param start_date: start of timeline segment to collect
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return timeline: timeline dictionary of user, None if one doesn't exist
    """
    # Determine wheter USER is a user id or user name, then return the appropriate URL
    assert ((USER_type == 'user_id') or (USER_type == 'screen_name')), "The parameter USER_type must be either 'user_id' or 'screen_name'"
    url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?{}={}&count=200'.format(USER_type, USER)
    cafile = _get_twitter_certificate()
    # Collect user timeline
    auth = _check_rate_limit_status(type='timeline', proxies=proxies, auth=auth)
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='timeline', proxies=proxies, auth=auth)
    try:
        timeline = r.json()
    except ValueError:
        print "ValueError: ", r
        return []
    # API Error (possibly due to privacy settings on user account)
    if isinstance(timeline, dict) and (('error' in timeline.keys()) or ('errors' in timeline.keys())):
        print "Timeline Error: ", timeline
        return []
    # Check if timeline is empty
    if len(timeline) < 1:
        print "\tThe timeline for user ", str(USER), " is empty. Move on to next user."
        return []
    # Store data in a dictionary named timeline
    try:
        max_id = timeline[len(timeline)-1]['id']
        oldest_date = _convert_twitter_date(timeline[len(timeline)-1]['created_at'])
    except KeyError:
        print "Key Error within timeline. Skip this timeline."
        return []
    # Continue collecting if start_date earlier than when the oldest tweet returned was published - keep collecting
    while (oldest_date-start_date).days > 0:
        # Make request for additional tweets
        auth = _check_rate_limit_status(type='timeline', proxies=proxies, auth=auth)
        r = requests.get(url='{}&max_id={}'.format(url, max_id), proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        _discount_remaining_calls(type='timeline', proxies=proxies, auth=auth)
        try:
            rr2 = r.json()
        except ValueError:
            print "\tThere are no more tweets to collect"
            break
        if isinstance(rr2, dict) and (('error' in rr2.keys()) or ('errors' in rr2.keys())):
            print "Timeline Error: ", rr2
            break
        if (rr2 != None) and (len(rr2) > 1):
            rr2.remove(rr2[0])          # Remove redundant tweet
            timeline = timeline + rr2   # Add tweets to struct
            # update new_date and max_id
            oldest_date = _convert_twitter_date(rr2[len(rr2)-1]['created_at'])
            max_id = rr2[len(rr2)-1]['id']
        else:
            print "\tThere are no more tweets to collect by user {}".format(USER)
            break
    # EOF return timeline if it exists
    return timeline

def pull_timeline_entitites(timeline, type, limit=None):
    """
    Pull fields out of a timeline array into a list.

    :param timeline: A Twitter timeline that has been loaded into Python (typically a dictionary format)
    :param type: Specify 'user_mentions' or 'in_reply_to_user_id' to get from the timelines. Example, type = ['text', 'geo']
    :param limit: Limit of entities to collect from timeline. Default is None which means to collect all
    :return: List that contains the specified field, ex. List of in_reply_to_user_id.
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


##
# FUNCTIONS FOR COLLECTING TWEET INFORMATION
def get_tweets(tweet_id, proxies, auth, include_entities='', trim_user='', keep_missing_twts=''):
    """
    Returns a list of  tweet dictionaries, specified by the id parameter. The Tweet's author will also be embedded
    within the tweet.

    :param tweet_id: Unique ID of tweet, or list of tweet IDs
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param include_entities: The entities node that may appear within embedded statuses will be disincluded when set to false.
    :param trim_user: When set to either true, each tweet returned in a timeline will include a user object including
        only the status authors numerical ID. Omit this parameter to receive the complete user object.
    :param keep_missing_twts: When using the map parameter, tweets that do not exist or cannot be viewed by the current
        user will still have their key represented but with an explicitly null value paired with it
    :return twts: List of tweet dictionaries
    """
    # Check parameters
    if (tweet_id is None) or (tweet_id == []): return []
    if not isinstance(tweet_id, list):
        tweet_id = [tweet_id]   # make this a list object
    params = {}
    if include_entities != '':
        assert (isinstance(include_entities, bool)), "The parameter 'include_entities' must be a boolean"
        params['include_entities'] = include_entities
    if trim_user != '':
        assert (isinstance(trim_user, bool)), "The parameter 'trim_user' must be a boolean"
        params['trim_user'] = trim_user
    if keep_missing_twts != '':
        assert (isinstance(keep_missing_twts, bool)), "The parameter 'keep_missing_twts' must be a boolean"
        params['map'] = keep_missing_twts
    # Split tweet IDs into groups of 100
    ids = [tweet_id[i:i+100] for i in range(0, len(tweet_id), 100)]
    # Prepare API call
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='statuses/lookup', proxies=proxies, auth=auth)     # Check RLS
    url = "https://api.twitter.com/1.1/statuses/lookup.json?id=" + ",".join([str(j) for j in ids[0]])
    if params != {}:
        url += '&' + urllib.urlencode(params)
    r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='statuses/lookup', proxies=proxies, auth=auth)
    twts = r.json()
    # Additional requests for user information, AKA more than 100 tweet IDs are requested
    if len(ids) > 1:
        for i in range(1, len(usernames)):
            auth = _check_rate_limit_status(type='statuses/lookup', proxies=proxies, auth=auth)
            url = "https://api.twitter.com/1.1/statuses/lookup.json?id=" + ",".join([str(j) for j in ids[i]])
            if params != {}:
                url += '&' + urllib.urlencode(params)
            r = requests.get(url=url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
            _discount_remaining_calls(type='statuses/lookup', proxies=proxies, auth=auth)
            twts += r.json()
    return twts

def get_retweets(tweet_id, proxies, auth, trim_user='', limit=100):
    '''
    Find all retweets of a given tweet, specified by a numerical tweet ID. Up to 100 retweets per request
    
    :param tweet_id: Unique ID of tweet
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param trim_user: When set to either true each tweet returned in a timeline will include a user object
            including only the status authors numerical ID. Omit this parameter to receive the complete user object.
    :param limit: limit to number of friends to collect. Max of 100 returned
    :return: Full returned json OR list of user IDs who have retweeted
    '''
    # Check parameters
    params = {'count': max(1, min(limit, 100))}
    if trim_user != '':
        assert isinstance(trim_user, bool), "The parameter 'trim_user' needs to be boolean"
        params['trim_user'] = trim_user
    cafile = _get_twitter_certificate()
    url = 'https://api.twitter.com/1.1/statuses/retweets/{}.json?'.format(tweet_id) + urllib.urlencode(params)
    # Call API to get retweets
    auth = _check_rate_limit_status(type='retweets', proxies=proxies, auth=auth)
    r = requests.get(url, proxies = proxies, auth = auth['OAUTH'], verify = cafile)
    _discount_remaining_calls(type='retweets', proxies=proxies, auth=auth)
    return r.json()


##
# SEARCH FOR TWEETS
def search_for_tweets(proxies, auth, limit=100, q='', exclusive=False, geocode='', lang='', result_type='mixed', until='', locale='', include_entities=''):
    """
    This function searches for tweets based on a combination of string queries, geocode, langauge, date or result types.
    Please note that Twitter's search service and, by extension, the Search API is not meant to be an exhaustive source
    of Tweets. Not all Tweets will be indexed or made available via the search interface.

    :param q: A string or list of strings to query. This function searches for hashtags as well
    :param exclusive: Boolean, if True, search with ORs rather than ANDs. Default is False
    :param geocode: Returns tweets by users located within a given radius of the given latitude/longitude. The parameter
                    value is specified by 'latitude,longitude,radius', where radius units must be specified as either
                    'mi' (miles) or 'km' (kilometers).
    :param lang: Restricts tweets to the given language, given by an ISO 639-1 code
    :param result_type: Specifies what type of search results you would prefer to receive. The default is 'mixed'
                        Valid values include 'mixed' (includes both popular and real time results in the response),
                        'recent' (return only the most recent results in the response) and 'popular' (return only the
                        most popular results in the response)
    :param limit: Number of tweets to collect. Set to None to get all possible tweets. The default is 100 tweets.
    :param until: Returns tweets created before the given date, which should be formatted as YYYY-MM-DD. No tweets will
                  be found for a date older than one week.
    :param locale: Specify the language of the query you are sending (only ja is currently effective). This is intended
                for language-specific consumers and the default should work in the majority of cases.
    :param include_entities: The entities node will be disincluded when set to false.
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    """
    # Check parameters
    params = {}
    # Set API calls based on limit
    if (limit is not None) and limit <= 100:
        limit = int(limit)
        params['count'] = str(limit)
    else:
        params['count'] = '100'
    # String query parameter
    if q != '':
        assert (isinstance(exclusive, bool)), "The parameter 'exclusive' must be a boolean"
        if isinstance(q, list):
            if exclusive:
                query = ' OR '.join(q)
            else:
                query = ' '.join(q)
        else:
            query = q
        params['q'] = query
    # Other parameters
    # if geocode != '':
    #     params['geocode'] = geocode
    if lang != '':
        params['lang'] = lang
    assert (result_type in ['mixed', 'recent', 'popular']), "You must set 'result_type' as either 'mixed', 'recent', or 'popular'"
    params['result_type'] = result_type
    if until != '':
        params['until'] = until
    if locale != '':
        params['locale'] = locale
    if include_entities != '':
        assert isinstance(include_entities, bool), "The parameter 'include_entities' must be boolean"
        params['include_entities'] = include_entites
    # Prepare for collection
    keepGoing = True
    print "\nReview tweet search params:"
    for k in params.keys():
        print "\t{}: {}".format(k, params[k])
    if geocode != '':
        print "\tgeocode: {}".format(geocode)
    cafile = _get_twitter_certificate()
    tweets = []
    while keepGoing:
        # Make API request
        url = 'https://api.twitter.com/1.1/search/tweets.json?' + urllib.urlencode(params)
        if geocode != '':
            url += '&geocode={}'.format(geocode)
        auth = _check_rate_limit_status(type='search_tweets', proxies=proxies, auth=auth)
        r = requests.get(url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        r = r.json()
        _discount_remaining_calls(type='search_tweets', proxies=proxies, auth=auth)
        try:
            # Get statuses
            tweets += r['statuses']
            # Update query
            params['max_id'] = r['search_metadata']['since_id_str']
        except:
            print "ERROR in searching for tweets: ", r
            return tweets
        if len(r['statuses']) < int(params['count']):
            keepGoing = False
        if (limit != None) and (len(tweets) >= limit):
            keepGoing = False
    return tweets

def get_tweets_with_hashtag(proxies, auth, limit=100, q='', exclusive=False, geocode='', lang='', result_type='mixed', until='', locale='', include_entities=''):
    """
    This function searches for tweets based on a combination of string hastag queries, geocode, langauge, date or result
    types. Please note that Twitter's search service and, by extension, the Search API is not meant to be an exhaustive
    source of Tweets. Not all Tweets will be indexed or made available via the search interface.

    :param q: A string or list of strings to query. This function searches for hashtags as well
    :param exclusive: Boolean, if True, search with ORs rather than ANDs. Default is False
    :param geocode: Returns tweets by users located within a given radius of the given latitude/longitude. The parameter
                    value is specified by 'latitude,longitude,radius', where radius units must be specified as either
                    'mi' (miles) or 'km' (kilometers).
    :param lang: Restricts tweets to the given language, given by an ISO 639-1 code
    :param result_type: Specifies what type of search results you would prefer to receive. The default is 'mixed'
                        Valid values include 'mixed' (includes both popular and real time results in the response),
                        'recent' (return only the most recent results in the response) and 'popular' (return only the
                        most popular results in the response)
    :param limit: Number of tweets to collect. Set to None to get all possible tweets. The default is 100 tweets.
    :param until: Returns tweets created before the given date, which should be formatted as YYYY-MM-DD. No tweets will
                  be found for a date older than one week.
    :param locale: Specify the language of the query you are sending (only ja is currently effective). This is intended
                for language-specific consumers and the default should work in the majority of cases.
    :param include_entities: The entities node will be disincluded when set to false.
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    """
    # Format hashtags
    if isinstance(q, list):
        for ii in range(len(q)):
            if not q[ii].startswith('#'):
                q[ii] = '#' + q[ii]
    else:
        if not q.startswith('#'):
            q = '#' + q
    # Search for tweets
    tweets = search_for_tweets(proxies=proxies, auth=auth, limit=limit, q=q, exclusive=exclusive, geocode=geocode,
                               lang=lang, result_type=result_type, until=until, locale=locale, include_entities=include_entities)
    return tweets


##
# SEARCH FUNCTIONS FOR USERS
def search_users(q, proxies, auth, limit=1000, exclusive=False, include_entities=''):
    """
    Provides a simple, relevance-based search interface to public user accounts on Twitter. Try querying by topical
    interest, full name, company name, location, or other criteria. Exact match searches are not supported. Only the
    first 1,000 matching results are available.

    :param q: Search term query, must be a string object or list of string objects
    :param exclusive: Boolean, if True, search with ORs rather than ANDs. Default is False
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param limit: limit to number of users to collect. Maximum and default values are 1000
    :param include_entities: The entities node will be disincluded from embedded tweet objects when set to false.
    :return profiles: list of profile dictionaries
    """
    # Check parameters
    params = {'count': '20'}
    # Format string query
    if isinstance(q, list):
        if exclusive:
            str_query = ' OR '.join(q)
        else:
            str_query = ' '.join(q)
    else:
        str_query = q
    params['q'] = str_query
    if include_entities != '':
        assert (isinstance(include_entities, bool)), "The parameter 'include_entities' must be a boolean"
        params['include_entities'] = include_entities
    # Set API calls based on limit
    if limit <= 20:
        keepLooking = False
        limit = int(limit)
    else:
        keepLooking = True
    # Prepare collection
    profiles = []
    page = 0
    cafile = _get_twitter_certificate()
    # Format API call
    params['page'] = str(page)
    url = "https://api.twitter.com/1.1/users/search.json?" + urllib.urlencode(params)
    # Call API to get retweets
    auth = _check_rate_limit_status(type='users_search', proxies=proxies, auth=auth)
    res = requests.get(url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='users_search', proxies=proxies, auth=auth)
    if not isinstance(res.json(), list):
        print "Error: ", res.json()
        return profiles
    else:
        profiles += res.json()
    # Keep looking, limit > 20
    while keepLooking and (len(profiles) < limit):
        page += 1
        params['page'] = str(page)
        url = "https://api.twitter.com/1.1/users/search.json?" + urllib.urlencode(params)
        auth = _check_rate_limit_status(type='users_search', proxies=proxies, auth=auth)
        res = requests.get(url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
        _discount_remaining_calls(type='users_search', proxies=proxies, auth=auth)
        if not isinstance(res.json(), list):
            print "Error: ", res.json()
            return profiles
        else:
            profiles += res.json()
        if len(res.json()) < 20:
            keepLooking = False
    # Remove extra followers from list
    if len(profiles) > limit:
        profiles = profiles[0:limit]
    return profiles


##
# GEOGRAPHY SEARCH FUNCTIONS
def reverse_geocode(lat, lon, proxies, auth, accuracy=0, granularity='neighborhood', limit=20):
    """
    Searches for up to 20 places that can be used as a place_id when updating a status. This request is an informative
    call and will deliver generalized results about geography.

    :param lat: The latitude to search around, which must be inside the range -90.0 to +90.0
    :param lon: The longitude to search around, which must be inside the range -180.0 to +180.0
    :param accuracy: The hint on the "region" in which to search, in meters, with a default value of 0m. If a number,
                then this is a radius in meters, but it can also take a string that is suffixed with ft to specify feet.
    :param granularity: This is the minimal granularity of place types: 'poi', 'neighborhood', 'city', 'admin' or
                'country'. Default is 'neighborhood'
    :param limit: Number of places to return, returns up to 20
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return: List of place dictionaries
    """
    assert (-90. <= lat <= 90.), "The 'lat' value must be in the range [-90, 90]"
    assert (-180. <= lon <= 180.), "The 'lon' value must be in the range [-180, 180]"
    granularity_levels = ['poi', 'neighborhood', 'city', 'admin', 'country']
    assert (granularity.lower() in granularity_levels), "You must set 'granularity' as one of the values: {}".format(", ".format(granularity_levels))
    limit = max(0, min(limit, 20))          # Clip limit
    # Load certificate and check rate limit status
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='geo_reverse', proxies=proxies, auth=auth)
    # Format API call
    params = {'lat': str(lat), 'long': str(lon), 'accuracy': str(accuracy), 'granularity': granularity, 'max_results': limit}
    url = "https://api.twitter.com/1.1/geo/reverse_geocode.json?" + urllib.urlencode(params)
    # Call API to get retweets
    res = requests.get(url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    res = res.json()
    _discount_remaining_calls(type='geo_reverse', proxies=proxies, auth=auth)
    try:
        return res['result']['places']
    except:
        print "ERROR in collecting place IDs: ", res
        return []

def lookup_place(place_id, proxies, auth):
    """
    Returns all the information about a known Twitter place, given its place ID. Twitter places are defined on the page
    https://dev.twitter.com/overview/api/places

    :param place_id: Unique ID assigned to place by Twitter, can be looked up by reverse_geocode()
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :return: Dictionary with place inforamtion
    """
    # Load certificate and check rate limit status
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='place_id_lookup', proxies=proxies, auth=auth)
    # Format API call
    url = "https://api.twitter.com/1.1/geo/id/{}.json".format(place_id)
    # Call API to get retweets
    res = requests.get(url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='place_id_lookup', proxies=proxies, auth=auth)
    return res.json()

def geocode_search(proxies, auth, lat='', long='', q='', exclusive=False, ip='', granularity='', accuracy='', place_id=''):
    """
    Search for places that can be attached to a statuses/update. Given a latitude and a longitude pair, an IP address,
    or a name, this request will return a list of all the valid places that can be used as the place_id when updating a
    status.

    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    :param lat: The latitude to search around, which must be inside the range -90.0 to +90.0
    :param lon: The longitude to search around, which must be inside the range -180.0 to +180.0
    :param q: Search term query, must be a string object or list of string objects
    :param exclusive: Boolean, if True, search with ORs rather than ANDs. Default is False
    :param ip: An IP address. Used when attempting to fix geolocation based off of the user's IP address.
    :param granularity: This is the minimal granularity of place types: 'poi', 'neighborhood', 'city', 'admin' or
            'country'. Default is 'neighborhood'
    :param accuracy: The hint on the "region" in which to search, in meters, with a default value of 0m. If a number,
            then this is a radius in meters, but it can also take a string that is suffixed with ft to specify feet.
    :param max_results: A hint as to the number of results to return. This does not guarantee that the number of results
            returned will equal max_results, but instead informs how many 'nearby' results to return
    :param place_id: This is the place_id which you would like to restrict the search results to. Setting this value
            means only places within the given place_id will be found.
    """
    # Check and format the parameters
    params = {}
    if (lat != '') and (long != ''):
        assert (-90. <= float(lat) <= 90.), "The 'lat' value must be in the range [-90, 90]"
        assert (-180. <= float(long) <= 180.), "The 'long' value must be in the range [-180, 180]"
        params['lat'] = str(lat)
        params['long'] = str(long)
    if q != '':
        if isinstance(q, list):
            if exclusive:
                query = ' OR '.join(q)
            else:
                query = ' '.join(q)
        else:
            query = q
        params['query'] = query
    if ip != '':
        params['ip'] = ip
    if granularity != '':
        granularity_levels = ['poi', 'neighborhood', 'city', 'admin', 'country']
        assert (granularity.lower() in granularity_levels), "You must set 'granularity' as one of the values: {}".format(", ".format(granularity_levels))
        params['granularity'] = granularity
    if accuracy != '':
        params['accuracy'] = str(accuracy)
    if place_id != '':
        params['place_id'] = place_id
    print "\nReview search geo params:"
    for k in params.keys():
        print "\t{}: {}".format(k, params[k])
    # Load certificate and check rate limit status
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='place_id_lookup', proxies=proxies, auth=auth)
    # Format API call
    url = "https://api.twitter.com/1.1/geo/search.json?" + urllib.urlencode(params)
    # Call API to get retweets
    res = requests.get(url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='place_id_lookup', proxies=proxies, auth=auth)
    res = res.json()
    try:
        return res['result']['places']
    except:
        print "ERROR in geo search: ", res
        return []


##
# TREND SEARCH FUNCTIONS
def find_trend_locations(proxies, auth):
    """
    Returns the locations that Twitter has trending topic information for. The response is an array of 'locations' that
    encode the location's WOEID and some other human-readable information.

    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    """
    # Load certificate and check rate limit status
    cafile = _get_twitter_certificate()
    auth = _check_rate_limit_status(type='available_trends', proxies=proxies, auth=auth)
    # Call API to get retweets
    res = requests.get("https://api.twitter.com/1.1/trends/available.json", proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='available_trends', proxies=proxies, auth=auth)
    return res.json()

def get_trends_for_place(proxies, auth, woeid=1, exclude=''):
    """
    Returns the top 50 trending topics for a specific WOEID, if trending information is available for it. The response
    is an array of 'trend' objects that encode the name of the trending topic, the query parameter that can be used to
    search for the topic on Twitter Search, and the Twitter Search URL. Use the function find_trend_places() to obtain
    a woeid

    The information is cached for 5 minutes, and requesting more frequently than that will not return any more data.

    :param woeid: The Yahoo! Where On Earth ID of the location to return trending information for. Global information
            is available by using 1 as the WOEID (default).
    :param exclude: Setting this equal to 'hashtags' will remove all hashtags from the trends list.
    :param proxies: proxy object, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    """
    # Check parameters
    params = {'id': str(woeid)}
    if exclude != '':
        assert (isinstance(exclude, str) and (exclude == 'hashtags')), "You must set the parameter exclude='hashtags', or leave it empty"
        params['exclude'] = exclude
    # Load certificate and check rate limit status
    cafile = _get_twitter_certificate()
    url = "https://api.twitter.com/1.1/trends/place.json?" + urllib.urlencode(params)
    auth = _check_rate_limit_status(type='/trends/place', proxies=proxies, auth=auth)
    res = requests.get(url, proxies=proxies, auth=auth['OAUTH'], verify=cafile)
    _discount_remaining_calls(type='/trends/place', proxies=proxies, auth=auth)
    return res.json()


# End of pyTweet.py
