#!/usr/bin/env python


"""
Authors: Kelly Geyer
Date: April 30, 2015
Installation: Python 2.7 on Windows 7

        File: build_network.py
Installation: Python 2.7 on Windows 7
      Author: Kelly Geyer
        Date: June 24, 2015

Description: This script builds a database based on a seed of users. After they are read into the program, each new
             hop of users consists of friends, followers, replies, and user mentions (vary by parameters set by user)
             of the first hop of users (excluding users who have already been added to network).
"""


import os, datetime, uuid, ujson, time, re, pyTweet, string, numpy as np, sys


def measure_data(user_dir, timeline_dir):
    '''
    This funtion measures the amount of data collected for the graph. Specifically it measures the files named
    'userInfo_*.json' in user_dir and files named 'timeline_*.json' in timeline_dir.

    @param user_dir - Directory storing profile .JSONs
    @param timeline_dir - Directory storing timeline .JSONs
    @return Gigabytes of data stored in user_dir and timeline_dir
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

def load_place_savers(user_dir):
    """
    This function loads variables that help pick up where the script left off: cur_user_list, next_user_list, cur_hop,
    and finished_users.

    @param user_dir - Directory where user profile JSON files are stored.
    @return A dictionary with variables used for saving the place of the collection loop, see the keys below:
        'finished_users' - dictionary where keys are profile ID strings and values are profile .JSON file paths
        'next_user_list' - set of user IDs to search for on the next hop
        'cur_user_list' - List of users that are part of the hop that's currently being collected.
        'cur_hop' - integer representing the current hop
    """
    # Load 'finished_users': make dictionary where keys are string ids and values are string uuid
    finished_users = {}
    jsons = os.listdir(user_dir)
    jsons = filter(lambda k: re.match('.+\.json', k), jsons)
    jsons = [os.path.join(user_dir, j) for j in jsons]
    for jj in jsons:
        try:
            if os.path.getsize(jj) == 0: continue
            jfid = open(os.path.join(user_dir, jj))
            profile = ujson.load(jfid)
            jfid.close()
            finished_users[str(profile['id'])] = os.path.basename(jj)[9:-5]
        except ValueError:
            continue
    # Load 'next_users_list'
    try:
        jfid = open(os.path.join(user_dir, 'next_user_list_v1.txt'))
        next_user_list = ujson.load(jfid)
        jfid.close()
    except ValueError:
        jfid = open(os.path.join(user_dir, 'next_user_list_v2.txt'))
        next_user_list = ujson.load(jfid)
        jfid.close()
    except IOError:
        print "The object next_user_list does not exist."
        next_user_list = []
    # Load 'cur_user_list'
    try:
        jfid = open(os.path.join(user_dir, 'cur_user_list_v1.txt'))
        cur_user_list = ujson.load(jfid)
        jfid.close()
    except ValueError:
        jfid = open(os.path.join(user_dir, 'cur_user_list_v2.txt'))
        cur_user_list = ujson.load(jfid)
        jfid.close()
    except IOError:
        print "The object cur_user_list does not exist."
        cur_user_list = []
    # Load 'cur_hop'
    try:
        jfid = open(os.path.join(user_dir, 'cur_hop_v1.txt'))
        cur_hop = ujson.load(jfid)
        jfid.close()
    except ValueError:
        jfid = open(os.path.join(user_dir, 'cur_hop_v2.txt'))
        cur_hop = ujson.load(jfid)
        jfid.close()
    except IOError:
        print "The object cur_hop does not exist."
        cur_hop = [0]
    cur_hop = cur_hop[0]
    return {'cur_user_list': set(cur_user_list), 'next_user_list': set(next_user_list), 'cur_hop': cur_hop, 'finished_users': finished_users}

def save_place_savers(user_dir, place_savers):
    """
    This function saves lists that help keep track of collected users, hop number, and users to search for the next hop.

    @param user_dir - Directory of profile .JSON
    @param place_savers
    """
    print "\tNOW SAVING PLACE SAVING PARAMETERS"
    # Save finished_users and next_user_list
    if 'cur_user_list' in place_savers.keys(): fast_save(filename=os.path.join(user_dir, 'cur_user_list_v1.txt'), obj=list(place_savers['cur_user_list']))
    if 'next_user_list' in place_savers.keys(): fast_save(filename=os.path.join(user_dir, 'next_user_list_v1.txt'), obj=list(place_savers['next_user_list']))
    if 'cur_hop' in place_savers.keys(): fast_save(filename=os.path.join(user_dir, 'cur_hop_v1.txt'), obj=[place_savers['cur_hop']])
    # Save back up of finished_users and next_user_list
    if 'cur_user_list' in place_savers.keys(): fast_save(filename=os.path.join(user_dir, 'cur_user_list_v2.txt'), obj=list(place_savers['cur_user_list']))
    if 'next_user_list' in place_savers.keys(): fast_save(filename=os.path.join(user_dir, 'next_user_list_v2.txt'), obj=list(place_savers['next_user_list']))
    if 'cur_hop' in place_savers.keys(): fast_save(filename=os.path.join(user_dir, 'cur_hop_v2.txt'), obj=[place_savers['cur_hop']])

def fast_save(filename, obj):
    """
    This function saves an object to a .txt file quickly using the module ujson.

    @param filename - name of file to save
    @param obj - Python object to save. This obj can eiher be a dictionary or list.
    """
    f = open(filename, 'w')
    ujson.dump(obj, f)
    f.close()

def breadth_first_search(user_seed, timeline_start_date, twitter_keys, host, port, save_dir={}, hop_out_limits={}, collection_limits={}):
    """
    This function creates a network based on Twitter friends

    @param user_seed           - List of user names
    @param twitter_keys        - Dictionary object containing 'API_KEY', 'API_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET'
    @param host                -
    @param port                -
    @param timeline_start_date - Beginning of date (datetime.date object) of timelines in collection
    @param save_dir     - Set locations for the profile and timeline directory to save .JSONs. The default will be your current working directory.
                          EX. save_dir = {'twitter_profiles': '/dir/to/save/profile/jsons',
                                          'twitter_timelines': '/dir/to/save/timeline/jsons'}
    @param hop_limits   - Specify your graph constrains with the variable hop_out_limits. First determine the maximum
                          number of hops to make the graph with 'max_hops', then decide the maximum amount of data to
                          collect in 'max_data'. This will be the combined profile and timeline .JSON files. Set it to
                          'None' if you don't want to limit the amount of data collected. Next, set limits (per
                          individual) on how many friends, followers, replied to users, and mentioned users to include
                          in the next hop. You can specify values [0, Inf) or None. Specifying 'None' implies that you
                          do not wish to limit the collection, and will expand the graph on as many as these edges as
                          possible. Occasionlly, you may get back fewer edges for a user than the limit you set. Note
                          that friends and followers will be saved in the fields 'friends_list' and 'followers_list'
                          automatically. The reply and mention users are saved in timelines.
                          EX. hop_out_limits = {'max_hops': 2,              # Maximin number of hops in graph
                                                'max_data': None,           # Maximum amount of data (in GB)
                                                'friends': 0,               # Maximum friends per user to include in next hop
                                                'followers': None,          # Maximum followers per user to include in next hop
                                                'in_reply_to_user_id': 17,  # Maximum 'in_reply_to_user_id' per user's timeline to include in next hop
                                                'user_mention_id': 21}      # Maximum 'user_mention_id' per user's timeline to include in next hop

    @param collection_limits - Suppose that you want to store friends or followers, but do not want to expand the graph
                    based on them. Specify limitations on collecting friends and followers below. Notice that reply and mention users
                    are saved in the timelines. The largest possible length of 'friends_list' will be the greater of hops out limit and
                    collection limit, or MAX(hops_out_limit['friends'], collection_limits['friends']). The same description follows for
                    'followers_list'.
                    EX. collection_limits = {'friends': 0,      # Maximum number of friends per user to save within the profile .JSON
                                         'followers': None}     # Maximum number of followers per user to save within the profile .JSON
    """
    # CHECK PARAMETERS
    # Check save_dir dictionary fields, create directories if they do not already exist
    if 'twitter_profiles' not in save_dir.keys():
        print "\tNo value was specified for save_dir['twitter_profiles'] so it will be set to {}.".format(os.getcwd())
        save_dir['twitter_profiles'] = os.getcwd()
    if not os.path.isdir(save_dir['twitter_profiles']):
        print "\tThe directory {} does not exist...creating it now".format(save_dir['twitter_profiles'])
        os.mkdir(save_dir['twitter_profiles'])
    if 'twitter_timelines' not in save_dir.keys():
        print "\tNo value was specified for save_dir['twitter_timelines'] so it will be set to {}.".format(os.getcwd())
        save_dir['twitter_timelines'] = os.getcwd()
    if not os.path.isdir(save_dir['twitter_timelines']):
        print "\tThe directory {} does not exist...creating it now".format(save_dir['twitter_timelines'])
        os.mkdir(save_dir['twitter_timelines'])
    # Check data amount and quit if graph has reached limit
    if ('max_data' in hop_out_limits) and (hop_out_limits['max_data'] is not None):
        data_vol = measure_data(user_dir=save_dir['twitter_profiles'], timeline_dir=save_dir['twitter_timelines'])
        if (data_vol > hop_out_limits['max_data']):
            print "The maximum amount of data has beek collected: {} GB with a limit of {} GB.".format(data_vol, hop_out_limits['max_data'])
            return
    # Check hop_out_limits dictionary
    if 'max_hops' not in hop_out_limits:
        hop_out_limits['max_hops'] = 6
        print "\tNo value was specified for hop_out_limits['max_hops'], the maximin number of hops in graph, so it will be set to {}.".format(hop_out_limits['max_hops'])
    if 'max_data' not in hop_out_limits:
        hop_out_limits['max_data'] = 2
        print "\tNo value was specified for hop_out_limits['max_data'], the maximin amount of data collected (in GB), so it will be set to {}.".format(hop_out_limits['max_data'])
    if 'friends' not in hop_out_limits:
        hop_out_limits['friends'] = 0
        print "\tNo value was specified for hop_out_limits['friends'], max friends per user to include in next hop, so it will be set to 0."
    if 'followers' not in hop_out_limits:
        hop_out_limits['followers'] = 0
        print "\tNo value was specified for hop_out_limits['followers'], max followers per user to include in next hop, so it will be set to 0."
    if 'in_reply_to_user_id' not in hop_out_limits:
        hop_out_limits['in_reply_to_user_id'] = 0
        print "\tNo value was specified for hop_out_limits['in_reply_to_user_id'], max 'in_reply_to_user_id' per user's timeline to include in next hop, so it will be set to 0."
    if 'user_mention_id' not in hop_out_limits:
        hop_out_limits['user_mention_id'] = 0
        print "\tNo value was specified for hop_out_limits['user_mention_id'], max 'user_mention_id' per user's timeline to include in next hop, so it will be set to 0."
    # Check collection_limits dictionary
    if 'friends' not in collection_limits:
        collection_limits['friends'] = 0
        print "\tNo value was specified for collection_limits['friends'], max number of friends per user to save with the profile .JSON, so it will be set to 0."
    if 'followers' not in collection_limits:
        collection_limits['followers'] = 0
        print "\tNo value was specified for collection_limits['followers'], max number of followers per user to save with the profile .JSON, so it will be set to 0."
    # DETERMINE COLLECTION PARAMETERS
    # Load place_savers dictionary
    print "\nGetting information of current hop and finished users..."
    place_savers = load_place_savers(save_dir['twitter_profiles'])
    print "\tAs of now {} user profiles have been collected and saved to {}".format(len(place_savers['finished_users']), save_dir['twitter_profiles'])
    print "\tThe current hop is {}".format(place_savers['cur_hop'])
    if place_savers['cur_hop'] < 1:
        place_savers['cur_user_list'] = set(user_seed)
    print "\tWe will collect {} users in hop {}".format(len(place_savers['cur_user_list']), place_savers['cur_hop'])
    print "\tSo far we plan to collect {} users in hop {}".format(len(place_savers['next_user_list']), place_savers['cur_hop'] + 1)
    print "\nfinished_users: ", place_savers['finished_users']
    print "\ncur_users: ", place_savers['cur_user_list']
    print "\nnext_users: ", place_savers['next_user_list']
    print "\nhop: ", place_savers['cur_hop']
    # Determine limits for friends/followers collection -
    if None in [hop_out_limits['friends'], collection_limits['friends']]:
        MAX_FRIENDS = None
    else:
        MAX_FRIENDS = max(hop_out_limits['friends'], collection_limits['friends'])
    if None in [hop_out_limits['followers'], collection_limits['followers']]:
        MAX_FOLLOWERS = None
    else:
        MAX_FOLLOWERS = max(hop_out_limits['followers'], collection_limits['followers'])
    # Create proxies dictionary
    proxies = {'http': 'http://%s:%s' % (host, port), 'https': 'http://%s:%s' % (host, port)}
    # API AUTHORIZATION
    print "\nAPI Authorization"
    auth = pyTweet.get_authorization(twitter_keys)
    # BUILD THE GRAPH
    print "\nStart building the graph!"
    for i in range(place_savers['cur_hop'], hop_out_limits['max_hops']):
        print "\nGet information for the {}th-hop users. There are {} total users in this hop.".format(i, len(place_savers['cur_user_list']))
        print "Create the user list of the " + str(i+1) + "th-hop users."
        # Remove finished_users from next_user_list
        if (place_savers['cur_hop'] > 0):
            place_savers['cur_user_list'].difference_update(set(map(int, place_savers['finished_users'].keys())))
        # Separate list for faster results, and delete place_savers['cur_user_list'] to free space
        USERS = [list(place_savers['cur_user_list'])[z:z+100] for z in range(0, len(place_savers['cur_user_list']), 100)]
        del place_savers['cur_user_list']   # save space
        for j in range(len(USERS)):
            # Look up information of users, 100 at a time
            print "\tLook up user information"
            if i < 1:
                # The initial list contain user names or @handles
                user_info = pyTweet.user_lookup_usernames(user_list=USERS[j], proxies=proxies, auth=auth)
                USERS[j] = set([])
                for jj in range(len(user_info)):
                    USERS[j].add(int(str(user_info[jj]['id'])))
            else:
                # All other lists will contain user ids
                user_info = pyTweet.user_lookup_userids(user_list=USERS[j], proxies=proxies, auth=auth)
            # Get friends, followers, and timelines of each user in user_info
            for k in range(len(user_info)):
                id = str(user_info[k]['id'])
                # Check to see that the user's friend/follower list hasn't already been collected
                if id in place_savers['finished_users'].keys():
                    # Load previously saved user data
                    pro_filename = os.path.join(save_dir['twitter_profiles'], 'userInfo_' + str(place_savers['finished_users'][id]) + '.json')
                    if os.path.getsize(pro_filename) == 0:
                        # File exists but it is empty
                        user_data = user_info[k]
                        user_data['khop'] = i
                        user_data['DOC'] = datetime.datetime.utcnow()
                        fast_save(filename=pro_filename, obj=user_data)
                    else:
                        try:
                            # Open and read profile .json
                            jfid = open(pro_filename)
                            user_data = ujson.load(jfid)
                            user_data['DOC'] = datetime.datetime.utcnow()
                            jfid.close()
                        except ValueError:
                            # Fail at opening profile .json, resave it
                            user_data = user_info[k]
                            user_data['khop'] = i
                            user_data['DOC'] = datetime.datetime.utcnow()
                            fast_save(filename=pro_filename, obj=user_data)
                else:
                    # The user's profile has not been collected...start now
                    place_savers['finished_users'][id] = str(uuid.uuid4())
                    pro_filename = os.path.join(save_dir['twitter_profiles'], 'userInfo_{}.json'.format(str(place_savers['finished_users'][id])))
                    # Add user information: hop, DOC
                    user_data = user_info[k]
                    user_data['khop'] = i
                    user_data['DOC'] = datetime.datetime.utcnow()
                    fast_save(filename=pro_filename, obj=user_data)
                print "\tSaved user {} information in {}.".format(id, pro_filename)
                # Collect user friends
                if 'friends_list' not in user_data:
                    friends_list = []
                    if (user_data['friends_count'] > 0) and ((MAX_FRIENDS is None) or (MAX_FRIENDS > 0)):
                        print "\tCollect friends for user {}.".format(id)
                        friends_list = pyTweet.get_user_friends(user_id=id, limit=MAX_FRIENDS, proxies=proxies, auth=auth)
                    user_data['friends_list'] = friends_list
                    fast_save(filename=pro_filename, obj=user_data)
                place_savers['next_user_list'].difference_update(set(user_data['friends_list'][0:hop_out_limits['friends']]))    # Add friends to next_user_list
                save_place_savers(user_dir=save_dir['twitter_profiles'], place_savers=place_savers)
                # Collect user followers
                if 'followers_list' not in user_data:
                    followers_list = []
                    if (user_data['followers_count'] > 0) and ((MAX_FOLLOWERS is None) or (MAX_FOLLOWERS > 0)):
                        print "\tCollect followers for user {}.".format(id)
                        followers_list = pyTweet.get_user_followers(user_id=id, limit=MAX_FOLLOWERS, proxies=proxies, auth=auth)
                    user_data['followers_list'] = followers_list
                    fast_save(filename=pro_filename, obj=user_data)
                place_savers['next_user_list'].difference_update(set(user_data['followers_list'][0:hop_out_limits['followers']]))  # Add followers to next_user_list
                save_place_savers(user_dir=save_dir['twitter_profiles'], place_savers=place_savers)
                # Collect timeline for user beginning from start_date
                tl_filename = os.path.join(save_dir['twitter_timelines'], 'timeline_{}.json'.format(place_savers['finished_users'][id]))
                if os.path.isfile(tl_filename):
                    print "\tThe timeline for user {} has already been collected.".format(id)
                    # Load timeline file
                    if os.path.getsize(tl_filename) == 0: continue      # Skip empty time lines
                    try:
                        jfid = open(tl_filename)
                        tldata = ujson.load(jfid)
                        jfid.close()
                    except (IOError, ValueError):
                        # Fail at opening file, recollect time line
                        print "\tCollect the timeline for user {}.".format(id)
                        tldata = pyTweet.collect_user_timeline(USER=id, USER_type='user_id', start_date=timeline_start_date, proxies=proxies, auth=auth)
                        for tl in range(len(tldata)):
                            tldata[tl]['DOC'] = datetime.datetime.utcnow()
                        fast_save(filename=tl_filename, obj=tldata)
                else:
                    print "\tCollect the timeline for user {}.".format(id)
                    tldata = pyTweet.collect_user_timeline(USER=id, USER_type='user_id', start_date=timeline_start_date, proxies=proxies, auth=auth)
                    for tl in range(len(tldata)):
                        tldata[tl]['DOC'] = datetime.datetime.utcnow()
                    fast_save(filename=tl_filename, obj=tldata)

                # Pull out user mentions, if applicable
                if ('user_mention_id' in hop_out_limits) and ((hop_out_limits['user_mention_id'] > 0) or (hop_out_limits['user_mention_id'] is None)):
                    print "\tAdd user mentionds to the next hop"
                    tl_mentions = pyTweet.pull_timeline_entitites(timeline=tldata, type='user_mention_id', limit=hop_out_limits['user_mention_id'])
                    place_savers['next_user_list'].update(tl_mentions)
                    save_place_savers(user_dir=save_dir['twitter_profiles'], place_savers=place_savers)
                # Pull out user replies, if applicable
                if ('in_reply_to_user_id' in hop_out_limits) and ((hop_out_limits['in_reply_to_user_id'] > 0) or (hop_out_limits['in_reply_to_user_id'] is None)):
                    print "\tAdd replies to the next hop"
                    tl_replies = pyTweet.pull_timeline_entitites(timeline=tldata, type='in_reply_to_user_id', limit=hop_out_limits['in_reply_to_user_id'])
                    place_savers['next_user_list'].update(tl_replies)
                    save_place_savers(user_dir=save_dir['twitter_profiles'], place_savers=place_savers)
                # Check data amount and quit if graph has reached limit
                if ('max_data' in hop_out_limits) and (hop_out_limits['max_data'] is not None):
                    data_vol = measure_data(user_dir=save_dir['twitter_profiles'], timeline_dir=save_dir['twitter_timelines'])
                    if (data_vol > hop_out_limits['max_data']):
                        print "The maximum amount of data has beek collected: {} GB with a limit of {} GB.".format(data_vol, hop_out_limits['max_data'])
                        return
        # Remove finished_users from place_savers['next_user_list']
        place_savers['next_user_list'].difference_update(set(map(int, place_savers['finished_users'].keys())))
        # Prepare for next iteration of hop
        place_savers['cur_user_list'] = place_savers['next_user_list'];
        place_savers['next_user_list'] = set([])
        place_savers['cur_hop'] += 1
        save_place_savers(user_dir=save_dir['twitter_profiles'], place_savers=place_savers)
        print "There are ", len(place_savers['cur_user_list']), " users in the next iteration of users."
    print "\nDone building graph!"

# End of build_network.py
