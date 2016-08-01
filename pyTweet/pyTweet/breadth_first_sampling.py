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

Description: This script builds a database based on a seed of users. After they are read into the program, each new
             hop of users consists of friends, followers, replies, and user mentions (vary by parameters set by user)
             of the first hop of users (excluding users who have already been added to network).
"""


import os, datetime, uuid, ujson, re, numpy as np, sys
import pyTweet   # Modules from pyTweet


##
# Miscellaneous sampling functions
def measure_data(user_dir, timeline_dir):
    '''
    This funtion measures the amount of data collected for the graph. Specifically it measures the files named
    'userInfo_*.json' in user_dir and files named 'timeline_*.json' in timeline_dir. Note that this measures file size
    and not the space on disk.

    :param user_dir: Directory storing profile .JSONs
    :param timeline_dir: Directory storing timeline .JSONs
    :return gb: Gigabytes of data stored in user_dir and timeline_dir
    '''
    total_data = 0
    jsons = os.listdir(user_dir)
    jsons = filter(lambda k: re.match('userInfo_.+\.json', k), jsons)
    jsons = [os.path.join(user_dir, j) for j in jsons]
    for f in jsons:
        total_data += os.path.getsize(os.path.join(user_dir, f))
    jsons = os.listdir(timeline_dir)
    jsons = filter(lambda k: re.match('timeline_.+\.json', k), jsons)
    jsons = [os.path.join(timeline_dir, j) for j in jsons]
    for f in jsons:
        total_data += os.path.getsize(os.path.join(timeline_dir, f))
    print "\nApproximately {} GB of data have been collected for this network".format((float(total_data) / 1073741824.0))
    return (float(total_data) / 1073741824.0)

def fast_save(filename, obj):
    """
    This function saves an object to a .txt file quickly using the module ujson.

    :param filename: name of file to save
    :param obj: Python object to save. This obj can eiher be a dictionary or list.
    """
    ujson.dump(obj, open(filename, 'w'))

def load_place_savers(user_dir):
    """
    This function loads the following place saving parameters:
    1. cur_hop - Current hop of collection algorithm
    2. cur_user_list - List of users collented during current hop
    3. next_user_list - List of users to collect on next hop
    4. added_topics_for_cur_hop - Topics added from current hop (if relevant to sampling method)
    5. unavailable_accounts - List of unavailable accounts
    6. finished_users - Users that have already been collected

    :param user_dir: Directory where profile information is saved
    :return place_saver_obj: Python dictionary of forementioned fields
    """
    # Load object
    try:
        jfid = open(os.path.join(user_dir, 'place_saver_v1.txt'))
        place_saver_obj = ujson.load(jfid)
        jfid.close()
    except ValueError:
        jfid = open(os.path.join(user_dir, 'place_saver_v2.txt'))
        place_saver_obj = ujson.load(jfid)
        jfid.close()
    except IOError:
        print "The object 'place_saver' does not exist, creating it now"
        place_saver_obj = {}
    # Make all necessary fields in case they don't already exist
    if 'cur_user_list' not in place_saver_obj.keys():
        place_saver_obj['cur_user_list'] = set([])
    if 'next_user_list' not in place_saver_obj.keys():
        place_saver_obj['next_user_list'] = set([])
    if 'cur_hop' not in place_saver_obj.keys():
        place_saver_obj['cur_hop'] = 0
    if 'added_topics_for_cur_hop' not in place_saver_obj.keys():
        place_saver_obj['added_topics_for_cur_hop'] = set([])
    if 'unavailable_accounts' not in place_saver_obj.keys():
        place_saver_obj['unavailable_accounts'] = set([])
    if 'finished_users' not in place_saver_obj.keys():
        place_saver_obj['finished_users'] = {}
    jsons = filter(lambda k: re.match('userInfo_*', k), os.listdir(user_dir))
    for jj in range(len(jsons)):
        if jj % 200 == 0:
            print "Check profile JSON {} of {}".format(jj+1, len(jsons))
        try:
            full_filename = os.path.join(user_dir, jsons[jj])
            if os.path.getsize(full_filename) == 0:
                continue
            jfid = open(full_filename)
            profile = ujson.load(jfid)
            jfid.close()
            if profile['id'] in place_saver_obj['finished_users'].keys():
                continue
            else:
                place_saver_obj['finished_users'][profile['id']] = jsons[jj]
        except ValueError:
            continue
    # Ensure that all fields are set objects
    for kk in place_saver_obj.keys():
        if (kk != 'finished_users') and (kk != 'cur_hop'):
            place_saver_obj[kk] = set(place_saver_obj[kk])
    return place_saver_obj

def save_place_savers(user_dir, place_saver_obj):
    """
    This function loads the following place saving parameters:
    1. cur_hop - Current hop of collection algorithm
    2. cur_user_list - List of users collented during current hop
    3. next_user_list - List of users to collect on next hop
    4. added_topics_for_cur_hop - Topics added from current hop (if relevant to sampling method)
    5. unavailable_accounts - List of unavailable accounts
    6. finished_users - Users that have already been collected

    :param user_dir: Directory where profile information is saved
    :param place_saver_obj: Python dictionary of forementioned fields
    """
    ujson.dump(place_saver_obj, open(os.path.join(user_dir, 'place_saver_v1.txt'), 'w'))
    ujson.dump(place_saver_obj, open(os.path.join(user_dir, 'place_saver_v2.txt'), 'w'))

def load_growth_params(user_dir):
    """
    This function loads growth params. These are text files that contain information for a single hop, such as users
    that composed the hop, the friends of the hop, and so on.

    :param user_dir: Location of place savers
    :return growth_params: Dictionary containing growth parameters, where keys are the file name and values are a set object
    """
    try:
        jfid = open(os.path.join(user_dir, 'growth_params_v1.txt'))
        growth_params = ujson.load(jfid)
        jfid.close()
    except ValueError:
        jfid = open(os.path.join(user_dir, 'growth_params_v2.txt'))
        growth_params = ujson.load(jfid)
        jfid.close()
    except IOError:
        growth_params = {}
    return growth_params

def save_growth_params(user_dir, growth_obj, cur_hop):
    """
    This function saves growth params. These are text files that contain information for a single hop, such as users
    that composed the hop, the friends of the hop, and so on.

    :param user_dir: Directory where growth parameters are saved
    :param growth_obj: Dictionary containing growth parameters, where keys are the file name and values are a set object
    :param cur_hop: Current hop of the sampling loop
    """
    for kk in growth_obj.keys():
        fast_save(os.path.join(user_dir, kk), growth_obj[kk])
        # Get file's hop count, and delete finished files from dictionary
        regex = re.compile(r'\d+')
        file_hop = int(regex.search(kk).group(0))
        if file_hop != int(cur_hop):
            growth_obj.pop(kk, None)
    # Save growth dictionary
    ujson.dump(growth_obj, open(os.path.join(user_dir, 'growth_params_v1.txt'), 'w'))
    ujson.dump(growth_obj, open(os.path.join(user_dir, 'growth_params_v2.txt'), 'w'))


##
# Breadth first search
def _save_profile_json(profile_struct, save_dir, khop):
    """
    This function saves a profile JSON file and returns its file name

    :param profile_struct: Dictionary of profile information
    :param save_dir: Directory to save profile JSONs
    :param khop: Hop count
    :return json_filename: String
    """
    # # Check type of profile_struct: If it's a str/unicode convert it to a dictionary object
    # if profile_struct is None:
    #     print "nonee"
    #     return
    # if isinstance(profile_struct, str) or isinstance(profile_struct, unicode):
    #     print "type of profile_struct: ", type(profile_struct)
    #     if profile_struct.strip(u' ') == u'':
    #         print "Empy proflie????"
    #         return
    #     q = ujson.loads(profile_struct)
    #     del profile_struct
    #     profile_struct = q
        # profie_struct = ujson.loads(unicode(profile_struct))
    # print type(profile_struct)
    # print profile_struct     # unicode string
    # t = datetime.datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S %z")
    # # print "t: ", t
    # print type(t)
    # print type(unicode(t))
    profile_struct['DOC'] = datetime.datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S %z")
    profile_struct['khop'] = khop
    profile_struct['has_timeline'] = None
    json_filename = "userInfo_{}.json".format(str(uuid.uuid4()))
    fast_save(filename=os.path.join(save_dir, json_filename), obj=profile_struct)
    return json_filename

def _save_timeline_json(user_id, filename, start_date, proxies, auth):
    """
    This function fixes a timeline JSON in case that its previous saving process was interrupted.

    :param user_id: Twitter user ID
    :param filename: Filename to save timeline JSON
    :param start_date: Start date of timeline, datetime object
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    """
    assert ('timeline' in filename), "The file is named {}. Are you sure that this is a valid timeline JSON?"
    tl_info = pyTweet.collect_user_timeline(USER=user_id, USER_type='user_id', start_date=start_date, proxies=proxies, auth=auth)
    for tt in tl_info:
        tt['DOC'] = datetime.datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S %z")
    fast_save(filename=filename, obj=tl_info)

def breadth_first_search(user_seed, timeline_start_date, host, port, save_dir={}, hop_limits={}):
    """
    This function creates a network based on Twitter friends

    :param user_seed: List of user names
    :param host: Your host IP
    :param port: Your port
    :param timeline_start_date: Beginning of date (datetime.date object) of timelines in collection
    :param save_dir: Set locations for the profile and timeline directory to save .JSONs. The default will be your current working directory.
                          EX. save_dir = {'twitter_profiles': '/dir/to/save/profile/jsons',
                                          'twitter_timelines': '/dir/to/save/timeline/jsons'}

    :param hop_limits: Specify your graph constrains with the variable hop_limits. First determine the maximum
                          number of hops to make the graph with 'max_hops', then decide the maximum amount of data to
                          collect in 'max_data'. This will be the combined profile and timeline .JSON files. Set it to
                          'None' if you don't want to limit the amount of data collected. Next, set limits (per
                          individual) on how many friends, followers, replied to users, and mentioned users to include
                          in the next hop. You can specify values [0, Inf) or None. Specifying 'None' implies that you
                          do not wish to limit the collection, and will expand the graph on as many as these edges as
                          possible. Occasionlly, you may get back fewer edges for a user than the limit you set. Note
                          that friends and followers will be saved in the fields 'friends_list' and 'followers_list'
                          automatically. The reply and mention users are saved in timelines.
                          EX.hop_limits = {'max_hops': 2,              # Maximin number of hops in graph
                                                'max_data': None,           # Maximum amount of data (in GB)
                                                'friends': 0,               # Maximum friends per user to include in next hop
                                                'followers': None,          # Maximum followers per user to include in next hop
                                                'in_reply_to_user_id': 17,  # Maximum 'in_reply_to_user_id' per user's timeline to include in next hop
                                                'user_mention_id': 21}      # Maximum 'user_mention_id' per user's timeline to include in next hop
    """
    # CHECK PARAMETERS
    # Check save_dir dictionary fields, create directories if they do not already exist
    if ('twitter_profiles' not in save_dir.keys()) or (save_dir['twitter_profiles'].strip() == ''):
        save_dir['twitter_profiles'] = os.path.join(os.getcwd(), 'profiles')
        print "\tNo directory was specified for save_dir['twitter_profiles'] so it will be set to {}.".format(save_dir['twitter_profiles'])
    if not os.path.isdir(save_dir['twitter_profiles']):
        print "\tThe directory {} does not exist...creating it now".format(save_dir['twitter_profiles'])
        os.mkdir(save_dir['twitter_profiles'])
    if ('twitter_timelines' not in save_dir.keys()) or (save_dir['twitter_timelines'].strip() == ''):
        save_dir['twitter_timelines'] = os.path.join(os.getcwd(), 'timelines')
        print "\tNo directory was specified for save_dir['twitter_timelines'] so it will be set to {}.".format(save_dir['twitter_timelines'])
    if not os.path.isdir(save_dir['twitter_timelines']):
        print "\tThe directory {} does not exist...creating it now".format(save_dir['twitter_timelines'])
        os.mkdir(save_dir['twitter_timelines'])
    # Checkhop_limits dictionary
    hop_limits_defaults = {'max_hops': [6, 'the maximin number of hops in graph'],
                           'max_data': [2, 'the maximin amount of data collected (in GB)'],
                           'friends': [0, 'max friends per user to include in next hop'],
                           'followers': [0, 'max followers per user to include in next hop'],
                           'in_reply_to_user_id': [0, "max 'in_reply_to_user_id' per user's timeline to include in next hop"],
                           'user_mention_id': [0, "max 'user_mention_id' per user's timeline to include in next ho"]}
    for kk in hop_limits_defaults.keys():
        if kk not in hop_limits:
            hop_limits[kk] = hop_limits_defaults[kk][0]
            print "\tNo Value was specified for hop_limits['{}'], {}, so it will be set to {}.".format(kk, hop_limits_defaults[kk][1], hop_limits_defaults[kk][0])
    # Check data amount and quit if graph has reached limit
    if ('max_data' in hop_limits) and (hop_limits['max_data'] is not None):
        data_vol = measure_data(user_dir=save_dir['twitter_profiles'], timeline_dir=save_dir['twitter_timelines'])
        if (data_vol > hop_limits['max_data']):
            print "The maximum amount of data has beek collected: {} GB with a limit of {} GB.".format(data_vol, hop_limits['max_data'])
            return

    # DETERMINE COLLECTION PARAMETERS
    # Load place_savers dictionary
    print "\nGetting information of current hop and finished users..."
    place_savers = load_place_savers(save_dir['twitter_profiles'])
    print "\tAs of now {} user profiles have been collected and saved to {}".format(len(place_savers['finished_users']), save_dir['twitter_profiles'])
    print "\tThe current hop is {}".format(place_savers['cur_hop'])
    if place_savers['cur_hop'] < 1:
        place_savers['cur_user_list'] = set(user_seed)
    save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)
    print "\tWe will collect {} users in hop {}".format(len(place_savers['cur_user_list']), place_savers['cur_hop'])
    # Load growth parametes
    growth_params = load_growth_params(save_dir['twitter_profiles'])
    # Create proxies dictionary
    proxies = {'http': 'http://%s:%s' % (host, port), 'https': 'http://%s:%s' % (host, port)}
    # Load twitter keys
    twitter_keys = pyTweet.load_twitter_api_key_set()
    # API authorization
    auth = pyTweet.get_authorization(twitter_keys)

    # BUILD THE GRAPH
    print "\nStart building the graph!"
    for khop in range(place_savers['cur_hop'], hop_limits['max_hops']):
        print "\nGet information for the {}th-hop users. There are {} total users in this hop.".format(khop, len(place_savers['cur_user_list']))
        print "Create the user list of the {}th-hop users as well.".format(khop+1)

        # Get profile information of users in cur_user_list
        print "\nCOLLECT PROFILE INFORMATION FOR THE CURRENT SET OF USERS"
        if khop < 1:
            # Find profiles to collect
            profiles_to_collect = set(place_savers['cur_user_list'])
            for json_filename in place_savers['finished_users'].values():
                data = ujson.load(open(os.path.join(save_dir['twitter_profiles'], json_filename), 'r'))
                if data['screen_name'] in place_savers['cur_user_list']:
                    profiles_to_collect.discard(data['screen_name'])
            # Collect and save profiles
            user_info = []
            if len(profiles_to_collect) > 0:
                print "\nstart collecting profiles: {} profiles".format(len(profiles_to_collect))
                user_info = pyTweet.user_lookup_usernames(user_list=list(profiles_to_collect), proxies=proxies, auth=auth)
                if isinstance(user_info, dict) and ('errors' in user_info.keys()):
                    print "\nThe initial seed cannot be collected..."
                    print "Twitter error message: ", user_info
                # Save profile information
                # print "user_info: ", user_info
                # print type(user_info)
                for udata in user_info:
                    # print "udata: ", udata
                    # print type(udata)
                    json_filename = _save_profile_json(profile_struct=udata, save_dir=save_dir['twitter_profiles'], khop=khop)
                    place_savers['finished_users'][udata['id']] = json_filename
            # Convert screen names to user IDs in cur_user_list, identify unavailable accounts as well
            all_screennames = {}        # Keys are screen names and values are file name
            jsons = filter(lambda k: re.match('userInfo_*', k), os.listdir(save_dir['twitter_profiles']))
            for jj in jsons:
                try:
                    full_filename = os.path.join(save_dir['twitter_profiles'], jj)
                    if os.path.getsize(full_filename) != 0:
                        jfid = open(full_filename)
                        profile = ujson.load(jfid)
                        jfid.close()
                        all_screennames[profile['screen_name']] = jj
                except ValueError:
                    continue
            # Get corresponding user IDs for each screen name in cur_user_list
            cur_user_list_ids = set([])
            for scn_name in profiles_to_collect.union(place_savers['cur_user_list']):
                if scn_name in all_screennames.keys():
                    jfid = open(os.path.join(save_dir['twitter_profiles'], all_screennames[scn_name]))
                    profile = ujson.load(jfid)
                    jfid.close()
                    if 'id' in profile:
                        cur_user_list_ids.add(int(profile['id']))
                    else:
                        place_savers['unavailable_accounts'].add(scn_name)
                else:
                    place_savers['unavailable_accounts'].add(scn_name)
            print cur_user_list_ids
            del profiles_to_collect
            place_savers['cur_user_list'] = set(cur_user_list_ids)
        else:
            # Collect and save profiles
            profiles_to_collect = set(place_savers['cur_user_list']).difference(set(map(int, place_savers['finished_users'].keys())))
            user_info = pyTweet.user_lookup_userids(user_list=list(profiles_to_collect), proxies=proxies, auth=auth)
            for udata in user_info:
                json_filename = _save_profile_json(profile_struct=udata, save_dir=save_dir['twitter_profiles'], khop=khop)
                place_savers['finished_users'][udata['id']] = json_filename
            # Update current user list, and identify unavailable accounts
            new_cur_user_list = set([])
            for uid in profiles_to_collect.union(set(place_savers['cur_user_list'])):
                if uid in place_savers['unavailable_accounts']:
                    continue
                if uid in place_savers['finished_users'].keys():
                    new_cur_user_list.add(uid)
                else:
                    place_savers['unavailable_accounts'].add(uid)
            place_savers['cur_user_list'] = set(new_cur_user_list)
            del new_cur_user_list

        # Save place saving variables
        growth_params['h{}_users.json'.format(khop)] = set(place_savers['cur_user_list'])
        save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=khop)
        save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)

        # Get timelines for each user in user_info
        print "\nCOLLECT TIME LINES FOR CURRENT SET OF USERS"
        for uid in place_savers['cur_user_list']:
            if uid in place_savers['finished_users'].keys():
                profile_filename = place_savers['finished_users'][uid]
                uuid_profile = os.path.basename(profile_filename)[9:-5]
                timeline_filename = os.path.join(save_dir['twitter_timelines'], "timeline_{}.json".format(uuid_profile))
                try:
                    tldata = ujson.load(open(profile_filename, 'r'))
                    tldata['id']
                except (IOError, KeyError):
                    # Collect user data
                    user_info = pyTweet.user_lookup_userids(user_list=[uid], proxies=proxies, auth=auth)
                    if (user_info is not dict) or ('id' not in user_info.keys()):
                        continue
                    json_filename = _save_profile_json(profile_struct=user_info[0], save_dir=save_dir['twitter_profiles'], khop=khop)
                    place_savers['finished_users'][uid] = json_filename
                if ('has_timeline' in tldata.keys()) and (tldata['has_timeline'] is True):
                    continue
                if not os.path.isfile(timeline_filename):
                    print "Collect the timeline for user {}.".format(uid)
                    tldata = pyTweet.collect_user_timeline(USER=uid, USER_type='user_id', start_date=timeline_start_date, proxies=proxies, auth=auth)
                    for tl in range(len(tldata)):
                        tldata[tl]['DOC'] = datetime.datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S %z")
                        tldata[tl]['has_timeline'] = True
                    fast_save(filename=profile_filename, obj=tldata)

        print "\nGet friends of each user in cur_user_list"
        if hop_limits['friends'] != 0:
            growth_params["h{}_friends.json".format(khop)] = set([])
            print "\nCOLLECT FRIENDS OF CURRENT USER SET"
            # print "place_savers['cur_user_list']: ", place_savers['cur_user_list']
            for jj in place_savers['cur_user_list']:
                profile_filename = os.path.join(save_dir['twitter_profiles'], place_savers['finished_users'][jj])
                try:
                    data = ujson.load(open(profile_filename, 'r'))
                    # print data['id']
                except (IOError, KeyError, TypeError):
                    user_info = pyTweet.user_lookup_userids(user_list=[uid], proxies=proxies, auth=auth)
                    if (user_info is not dict) or ('id' not in user_info.keys()):
                        continue
                    _save_profile_json(profile_struct=user_info[0], save_dir=save_dir['twitter_profiles'], khop=khop)
                    json_filename = _save_profile_json(profile_struct=user_info[0], save_dir=save_dir['twitter_profiles'], khop=khop)
                    place_savers['finished_users'][uid] = json_filename
                if data['friends_count'] < 1:
                    data['friends_list'] = []
                    fast_save(filename=profile_filename, obj=data)
                    continue
                if 'friends_list' not in data.keys():
                    print "Collect friends for user {}".format(jj)
                    friends_list = pyTweet.get_user_friends(user_id=jj, limit=hop_limits['friends'], proxies=proxies, auth=auth)
                    data['friends_list'] = friends_list
                    fast_save(filename=profile_filename, obj=data)
                if hop_limits['friends'] < len(data['friends_list']):
                    place_savers['next_user_list'].update(set(data['friends_list'][0:len(hop_limits['friends'])-1]))
                else:
                    place_savers['next_user_list'].update(set(data['friends_list']))
                growth_params["h{}_friends.json".format(khop)].update(set(data['friends_list']))
                save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)
            save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=khop)

        print "\nGet followers of each user in the cur_user_list"
        if hop_limits['followers'] != 0:
            growth_params["h{}_followers.json".format(khop)] = set([])
            print "\nCOLLECT FOLLOWERS OF CURRENT USER SET"
            for jj in place_savers['cur_user_list']:
                # profile_filename = place_savers['finished_users'][jj]
                profile_filename = os.path.join(save_dir['twitter_profiles'], place_savers['finished_users'][jj])
                try:
                    data = ujson.load(open(profile_filename, 'r'))
                    data['id']
                except (IOError, KeyError):
                    user_info = pyTweet.user_lookup_userids(user_list=[uid], proxies=proxies, auth=auth)
                    if (user_info is not dict) or ('id' not in user_info.keys()):
                        continue
                    _save_profile_json(profile_struct=user_info[0], save_dir=save_dir['twitter_profiles'], khop=khop)
                if data['followers_count'] < 1:
                    data['followers_list'] = []
                    fast_save(filename=profile_filename, obj=data)
                    continue
                if 'followers_list' not in data.keys():
                    print "Collect followers for user {}".format(jj)
                    friends_list = pyTweet.get_user_friends(user_id=jj, limit=hop_limits['followers'], proxies=proxies, auth=auth)
                    data['followers_list'] = friends_list
                    fast_save(filename=profile_filename, obj=data)
                if hop_limits['followers'] < len(data['followers_list']):
                    place_savers['next_user_list'].update(set(data['followers_list'][0:len(hop_limits['followers'])-1]))
                else:
                    place_savers['next_user_list'].update(set(data['followers_list']))
                growth_params["h{}_followers.json".format(khop)].update(set(data['followers_list']))
                save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)
            save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=khop)

        # Pull out user mentions
        if ('user_mention_id' in hop_limits) and (hop_limits['user_mention_id'] != 0):
            print "\nCOLLECT USER MENTIONS OF CURRENT SET"
            growth_params["h{}_user_mentions.json".format(khop)] = set([])
            for jj in place_savers['cur_user_list']:
                profile_filename = place_savers['finished_users'][jj]
                uuid_profile = os.path.basename(profile_filename)[9:-5]
                timeline_filename = os.path.join(save_dir['twitter_timelines'], "timeline_{}.json".format(uuid_profile))
                # Load or create the timeline JSON file
                if os.path.isfile(timeline_filename):
                    if os.path.getsize(timeline_filename) == 0:
                        continue
                    # Load the timeline data
                    try:
                        tldata = ujson.load(open(timeline_filename, 'r'))
                        if len(tldata) < 1:
                            continue
                        tldata[0]['text']
                    except (IOError, KeyError):
                        # Fix timeline file
                        _save_timeline_json(user_id=jj, filename=timeline_filename, start_date=timeline_start_date, proxies=proxies, auth=auth)
                        tldata = ujson.load(open(timeline_filename, 'r'))
                else:
                    # Get the timeline data
                    _save_timeline_json(user_id=jj, filename=timeline_filename, start_date=timeline_start_date, proxies=proxies, auth=auth)
                    if os.path.getsize(timeline_filename) == 0:
                        continue
                    tldata = ujson.load(open(timeline_filename, 'r'))
                    if len(tldata) < 1:
                        continue
                # Pull out user mentions
                tl_mentions = pyTweet.pull_timeline_entitites(timeline=tldata, type='user_mention_id', limit=hop_limits['user_mention_id'])
                growth_params["h{}_user_mentions.json".format(khop)].update(tl_mentions)
                place_savers['next_user_list'].update(tl_mentions)
                save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)
            save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=khop)

        # Pull out replies
        if ('in_reply_to_user_id' in hop_limits) and (hop_limits['in_reply_to_user_id'] != 0):
            print "\nCOLLECT USERS CURRENT SET REPLIES TO"
            growth_params["h{}_replies.json".format(khop)] = set([])
            for jj in place_savers['cur_user_list']:
                profile_filename = place_savers['finished_users'][jj]
                uuid_profile = os.path.basename(profile_filename)[9:-5]
                timeline_filename = os.path.join(save_dir['twitter_timelines'], "timeline_{}.json".format(uuid_profile))
                # Load or create the timeline JSON file
                if os.path.isfile(timeline_filename):
                    if os.path.getsize(timeline_filename) == 0:
                        continue
                    # Load the timeline data
                    try:
                        tldata = ujson.load(open(timeline_filename, 'r'))
                        tldata[0]['text']
                    except (IOError, KeyError):
                        # Fix timeline file
                        _save_timeline_json(user_id=jj, filename=timeline_filename, start_date=timeline_start_date, proxies=proxies, auth=auth)
                        tldata = ujson.load(open(timeline_filename, 'r'))
                    if len(tldata) < 1:
                        continue
                else:
                    # Get the timeline data
                    _save_timeline_json(user_id=jj, filename=timeline_filename, start_date=timeline_start_date, proxies=proxies, auth=auth)
                    if os.path.getsize(timeline_filename) == 0:
                        continue
                    tldata = ujson.load(open(timeline_filename, 'r'))
                    if len(tldata) < 1:
                        continue
                # Pull out replies
                tl_replies = pyTweet.pull_timeline_entitites(timeline=tldata, type='in_reply_to_user_id', limit=hop_limits['in_reply_to_user_id'])
                place_savers['next_user_list'].update(tl_replies)
                growth_params["h{}_replies.json".format(khop)].update(tl_replies)
                save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)
            save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=khop)

        # Check data limit
        if ('max_data' in hop_limits) and (hop_limits['max_data'] is not None):
            data_vol = measure_data(user_dir=save_dir['twitter_profiles'], timeline_dir=save_dir['twitter_timelines'])
            if (data_vol > hop_limits['max_data']):
                print "The maximum amount of data has beek collected: {} GB with a limit of {} GB.".format(data_vol, hop_limits['max_data'])
                return
        # Prepare for next iteration
        place_savers['cur_hop'] = khop + 1
        place_savers['cur_user_list'] = set(place_savers['next_user_list'])
        place_savers['next_user_list'] = set([])
        save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)

