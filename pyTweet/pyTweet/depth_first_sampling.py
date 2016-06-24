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
Date: May 3, 2016
Installation: Python 2.7 on Windows 7

This script implements Twitter data collection methods relying on PostgreSQL
"""


import os, re, csv, uuid, ujson as jsn, datetime, sys, itertools, dateutil
import breadth_first_sampling, pyTweet, json_to_database, candid_tfidf

import psycopg2, psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


##
# Depth first search
def _get_profiles_wrapper(cur, conn, user_list, proxies, auth, list_type, hop):
    """
    This function is a wrapper for grabbing user profiles

    :param cur: Cursor to database
    :param conn: Connection to database
    :param user_list: List of Twitter user IDs
    :param list_type: must be 'ID' for user IDs or 'SN' for screen names
    :param get_profiles: Set object of users to collect
    """
    assert ((list_type == 'user_id') or (list_type == 'screen_name')), "The parameter must list_type must be set to either 'user_id' or 'screen_name'"
    db_user_ids = set([])       # List of redundant profiles
    # Filter out profiles that have already been collected
    cur.execute("SELECT DISTINCT {} FROM users;".format(list_type))
    q = cur.fetchall()      # q = [('sn',), ..., ('sn2',)]
    for ii in q:
        db_user_ids.add(ii[0])
    # Filter out deleted or protected profiles
    cur.execute("SELECT DISTINCT {} FROM lost_profiles;".format(list_type))
    q = cur.fetchall()
    for ii in q:
        db_user_ids.add(ii[0])
    get_profiles = list(set(user_list).difference(db_user_ids))
    if get_profiles is None:
        return
    # Partition IDs
    USERS = [get_profiles[z:z+100] for z in range(0, len(get_profiles), 100)]
    get_profiles = set(get_profiles)
    # del get_profiles    # Save space
    for j in range(len(USERS)):
        # Look up information of users, 100 at a time
        print "\tLook up profile information for up to 100 users at a time"
        if list_type == 'screen_name':
            user_info = pyTweet.user_lookup_usernames(user_list=list(USERS[j]), proxies=proxies, auth=auth)
            lost_cmd = "INSERT INTO lost_profiles (screen_name) VALUES ('{}');"
        elif list_type == 'user_id':
            user_info = pyTweet.user_lookup_userids(user_list=list(USERS[j]), proxies=proxies, auth=auth)
            lost_cmd = "INSERT INTO lost_profiles (user_id) VALUES ({});"
        else:
            print "The type '{}' is not recognized. Set list_type to either 'user_id' or 'screen_name'".format(list_type)
            return
        # Are there profiles that are either protected/deleted?
        if (not isinstance(user_info, list)) and ('errors' in user_info.keys()) and (user_info['errors'][0]['code'] == 17):
            for u in USERS[j]:
                json_to_database.make_sql_edit(cur, conn, lost_cmd.format(u))
                get_profiles.remove(u)
            return
        if len(user_info) < len(USERS[j]):
            for u in USERS[j]:
                profile_collected = False
                for ui in user_info:
                    if (u in ui.values()) or (str(u) in ui.values()):
                        profile_collected = True
                        break
                # Add profile to table deleted_profiles if necessary
                if not profile_collected:
                    json_to_database.make_sql_edit(cur, conn, lost_cmd.format(u))
                    get_profiles.remove(u)
        # Add user info to database
        for k in user_info:
            if k == 'errors':
                continue
            k['khop'] = hop
            k['DOC'] = datetime.datetime.utcnow()
            if hop < 1:
                k['expand_user'] = True
            json_to_database.add_user(userdata=k, cur=cur, conn=conn)

def _get_timeline_wrapper(cur, conn, user_id, tl_start_date, proxies, auth):
    """
    This function is a wrapper for grabbing user timelines.

    :param cur: Cursor to database
    :param conn: Connection to databaase
    :param user_id: Twitter user ID
    :param tl_start_date: Start date of timeline, datetime object
    :param proxies: proxy dictionary, ex. {'http': 'http://%s:%s' % (HOST, PORT), 'https': 'http://%s:%s' % (HOST, PORT)}
    :param auth: Twitter application authentication, see the get_authorization method
    """
    if (user_id is None):
        return
    if isinstance(user_id, basestring) and (user_id.strip() == ''):
        return
    # Has timeline already been collected?
    try:
        cur.execute("SELECT expand_user, has_timeline FROM users WHERE user_id = {};".format(user_id))
        q = cur.fetchone()
    except psycopg2.ProgrammingError:
        print "strange programming error"
        print "user id is ", user_id
        print type(user_id)
        # sys.utc(1)
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute("SELECT expand_user, has_timeline FROM users WHERE user_id = {};".format(user_id))
        q = cur.fetchone()
    if (q is None) or (q[0] is False) or (q[1] is not None):
        # User is not in database, or user shouldn't be expanded, or TL has already been collected
        return
    # Is the user's profile protected or deleted?
    cur.execute("SELECT profile_id FROM lost_profiles WHERE user_id = {};".format(user_id))
    qq = cur.fetchone()
    if qq is not None:
        return
    # check to see if user already has timeline
    if (q[0] is not False) and (q[1] is None):
        print '\tGet user timeline for user ', user_id
        TL = pyTweet.collect_user_timeline(USER=user_id, USER_type='user_id', start_date=tl_start_date, proxies=proxies, auth=auth)
        # Ignore empty TL
        if TL == []:
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET has_timeline=FALSE, timeline_is_relevant=FALSE WHERE user_id = {};".format(user_id))
            return
        # add date of collection to time line
        for tl in range(len(TL)):
            TL[tl]['DOC'] = datetime.datetime.utcnow()
        # Update has_tl
        tweetAdded = json_to_database.add_timeline(cur=cur, conn=conn, tldata=TL)
        if tweetAdded:
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET has_timeline = TRUE WHERE user_id = {};".format(user_id))
        else:
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET has_timeline = FALSE, timeline_is_relevant = FALSE WHERE user_id = {};".format(user_id))

def _filter_by_timeline(cur, conn, tl_start_date, tl_end_date, khop):
    """
    This function expands relevant users.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param tl_start_date: Timeline start date, datetime object
    :param tl_end_date: Timeline end date, datetime object
    :param khop: Current hop of sampling loop
    """
    # Get topic query
    topic_query = _get_topic_query(cur)
    # Get users/tweets, and expand users with relevant tweets
    cur.execute(cur.mogrify("SELECT DISTINCT (users.user_id) FROM users INNER JOIN tweets ON users.user_id=tweets.user_id WHERE (users.has_timeline_filter IS NULL) AND (users.expand_user IS NULL) AND (users.has_timeline = TRUE) AND (users.khop = {}) AND (tweets.created_at >= %s AND tweets.created_at <= %s);".format(khop), (tl_start_date, tl_end_date)))
    uids = cur.fetchall()
    for u in range(len(uids)):
        if u % 50 == 0:
            print "\nFilter timeline for user {}: {} out of {}".format(uids[u][0], u, len(uids))
        cur.execute(cur.mogrify("SELECT COUNT(tweet_id) FROM tweets WHERE (user_id = " + str(uids[u][0]) + ") AND (created_at >= %s AND created_at <= %s)", (tl_start_date, tl_end_date)) + " AND ({});".format(topic_query))
        q = cur.fetchone()[0]
        if q < 1:
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user = FALSE WHERE user_id = {};".format(uids[u][0]))
        json_to_database.make_sql_edit(cur, conn, "UPDATE users SET has_timeline_filter = TRUE WHERE user_id = {};".format(uids[u][0]))

def _get_topic_query(cur):
    """
    This function creates an OR-like SQL statement to see if a tweet contains a topic. The resulting string can
    be included in a SQL command to determine if a tweet is relevant.

    :param cur: Cursor for database
    :return topic_query_str: ex. "tweet ~* '\mtopic1\M' OR tweet ~* '\mtopic2\M' OR ... OR tweet ~* '\mtopicN\M'"
    """
    topic_queries = []
    cur.execute("SELECT topic FROM topics;")
    for t in cur:
        if 'http' in str(t[0]):
            topic_queries.append("tweet LIKE '%{}%' OR '{}' = ANY(url_entities)".format(str(t[0]), str(t[0])))
        else:
            topic_queries.append("tweet ~* '\m" + str(t[0]) + "\M' OR LOWER('" + str(t[0]) + "') = ANY(hashtag_entities)")
    return " OR ".join(topic_queries)

def _add_new_topics(cur, conn, tl_start_date, tl_end_date, hop):
    """
    This function adds new topics to the topic table after a hop has been collected. If a tweet contains at least one
    of the topic-like words, then its non-stop-word terms are added to the topic table.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param tl_start_date: Timeline start date, datetime object
    :param tl_end_date: Timeline end date, datetime object
    :param hop: Current hop count
    """
    # Count current topics in table
    cur.execute("SELECT COUNT(*) FROM topics;")
    init_nTopics = cur.fetchone()[0]
    # Get topic query for previous loop
    topic_query = _get_topic_query(cur)
    # Get users/tweets
    cur.execute(cur.mogrify("SELECT DISTINCT (users.user_id) FROM users INNER JOIN tweets ON users.user_id=tweets.user_id WHERE (users.expand_user = TRUE) AND (users.khop = %s) AND (users.has_timeline = TRUE) AND (users.friends_count > 0 OR users.followers_count > 0 OR tweets.retweet_count > 0) AND (tweets.created_at >= %s AND tweets.created_at <= %s);", (hop, tl_start_date, tl_end_date)))
    users = cur.fetchall()  # [(1,), (2,), ...]
    print "\tAdd new topics from {} user timelines ...".format(len(users))
    for u in users:
        print "\t\tAdd topics from user: ", u[0]
        cmd = "SELECT DISTINCT tweet_id FROM tweets WHERE user_id = %s AND (created_at >= %s AND created_at <= %s) AND (" + topic_query + ");"
        cur.execute(cur.mogrify(cmd, (u[0], tl_start_date, tl_end_date)))
        tweet_ids = cur.fetchall()
        print "\t\tThis user has {} relevant tweets to consider.".format(len(tweet_ids))
        for t in tweet_ids:
            # Label tweet as relevant
            json_to_database.make_sql_edit(cur, conn, "UPDATE tweets SET tweet_is_relevant = TRUE WHERE tweet_id ={};".format(t[0]))
            # Load tweet data
            cur.execute("SELECT * FROM tweets WHERE tweet_id = {};".format(t[0]))
            twt_data = cur.fetchone()
            # Add hashtags to topic table
            if twt_data['hashtag_entities'] is not None:
                new_hash = candid_tfidf.process_word_list(twt_data['hashtag_entities'])
                for h in new_hash:
                    json_to_database.make_sql_edit(cur, conn, cur.mogrify("INSERT INTO topics (topic, khop) VALUES (%s, %s);", (h, hop)))
            # Add URLs to topic table
            if twt_data['url_entities'] is not None:
                for url in twt_data['url_entities']:
                    json_to_database.make_sql_edit(cur, conn, cur.mogrify("INSERT INTO topics (topic, khop) VALUES (%s, %s);", (url, hop)))
            # Add nouns
    cur.execute("SELECT COUNT(*) FROM topics;")
    nTopics = cur.fetchone()[0]
    print "\t\tAdded {} new topics to the table.".format(nTopics - init_nTopics)

def depth_first_causal_search(user_seed, topic_seed, tl_start_date, tl_end_date, postgres_params, host, port, save_dir={}, hop_limits={}, collection_limits={}):
    """
    This funciton builds a network based on users relevant to seed keywords
    Requires that a PostgreSQL database already exist

    :param user_seed: List of user names
    :param topic_seed: List of seed topics
    :param tl_start_date: Beginning of date (datetime.date object) of timelines in collection
    :param tl_end_date: End date (datetime.date object) of timelines in collection
    :param postgres_params: Dictionary containing the fields '', ..., required to connect to a database
    :param host:
    :param port:
    :param save_dir: Directory to save sampling and growth parameters
    :param hop_limits: Specify your graph constrains with the variable hop_limits. Set the maximum number of hops to
                       make a graph with 'max_hops'.
                          EX. hop_limits = {'max_hops': 2}              # Maximum number of hops in graph
    :param collection_limits: Specify the term-frequency calculation and threshold percentile
                    EX. collection_limits = {'threshold_percentile': 0.05,  # Threshold percentile for ....
                                         'tf_type': 'raw'}     # TF caclulation type
    """
    # CHECK PARAMETERS
    print "\nCheck parameters"
    # Timeline start and end dates
    assert (isinstance(tl_start_date, datetime.date) and isinstance(tl_end_date, datetime.date)), "Both tl_start_date and tl_end_date must be datetime.date objects (i.e. tl_start_date = datetime.date(year=2014, month=1, day=1))."
    assert ((tl_end_date - tl_start_date) > datetime.timedelta(0)), "The end date must be later than the start date. Check the assignments of tl_start_date and tl_end_date."
    # Check PostgreSQL parameters
    assert (('dbname' in postgres_params.keys()) and ('user' in postgres_params.keys()) and ('password' in postgres_params.keys())), "Verify the parameters. The possible fields are 'dbname', 'user', 'password', 'host', and 'port'."
    try:
        conn = psycopg2.connect(" ".join(map(lambda x,y: "{}='{}'".format(x,y), postgres_params.keys(),postgres_params.values())))
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.OperationalError:
        print "OperationalError: Check your login credentials.  Make sure the database exists as well."
        return
    # Check hop_limits dictionary
    if 'max_hops' not in hop_limits:
        hop_limits['max_hops'] = 5
        print "\tNo value was specified for hop_limits['max_hops'], the maximin number of hops in graph, so it will be set to {}.".format(hop_limits['max_hops'])
    # Check save_dir dictionary fields, create directories if they do not already exist
    if ('twitter_profiles' not in save_dir.keys()) or (save_dir['twitter_profiles'].strip() == ''):
        save_dir['twitter_profiles'] = os.path.join(os.getcwd(), 'profiles')
        print "\tNo directory was specified for save_dir['twitter_profiles'] so it will be set to {}.".format(save_dir['twitter_profiles'])
    if not os.path.isdir(save_dir['twitter_profiles']):
        print "\tThe directory {} does not exist...creating it now".format(save_dir['twitter_profiles'])
        os.mkdir(save_dir['twitter_profiles'])
    # Check collection_limits dictionary
    if 'threshold_percentile' not in collection_limits:
        collection_limits['threshold_percentile'] = 0.05
        print "\tNo value was specified for collection_limits['threshold_percentile'], xxx, so it will be set to 0.05."
    assert (0 <= collection_limits['threshold_percentile'] <= 1), "The value collection_parameters['threshold_percentile'] must fall within [0,1]."
    if 'tf_type' not in collection_limits:
        collection_limits['tf_type'] = 'raw'
        print "\tNo value was specified for collection_limits['tf_type'], method of calculating the term frequency, so it will be set to 'raw'."
    assert ((collection_limits['tf_type'] == 'raw') or (collection_limits['tf_type'] == 'augmented') or (collection_limits['tf_type'] == 'boolean')), "The value collection_parameters['tf_type'] is not recognized. Please enter 'raw', 'boolean' or 'augmented' as it's value."

    # SET UP SECONDARY PARAMETERS
    # Create proxies dictionary
    proxies = {'http': 'http://%s:%s' % (host, port), 'https': 'http://%s:%s' % (host, port)}
    # Load twitter keys
    twitter_keys = pyTweet.load_twitter_api_key_set()

    # Load place_savers dictionary
    print "\nGetting information of current hop and finished users..."
    place_savers = breadth_first_sampling.load_place_savers(save_dir['twitter_profiles'])
    print "\tAs of now {} user profiles have been collected and saved to {}".format(len(place_savers['finished_users']), save_dir['twitter_profiles'])
    print "\tThe current hop is {}".format(place_savers['cur_hop'])
    if place_savers['cur_hop'] < 1:
        place_savers['cur_user_list'] = set(user_seed)
    print "\tWe will collect {} users in hop {}".format(len(place_savers['cur_user_list']), place_savers['cur_hop'])

    # Load growth parameters
    growth_params = breadth_first_sampling.load_growth_params(save_dir['twitter_profiles'])

    # API AUTHORIZATION
    print "\nAPI Authorization"
    OAUTH = pyTweet.get_authorization(twitter_keys)
    print "Start with key {}".format(OAUTH['KEY_FILE'])

    # CONFIGURE SCHEMA FOR TF-IDF ANALYSIS
    print "\nConfigure database for TF-IDF analysis"
    json_to_database.configure_database_to_build_network(cur, conn)
    # Load topics
    for t in topic_seed:
        if (t is None) or (t.strip() == ''):
            continue
        json_to_database.make_sql_edit(cur, conn, "INSERT INTO topics (topic, khop) VALUES ('{}', -1);".format(t.strip()))
    # Add columns for this sampling method
    new_columns = [{'table': 'users', 'col': 'has_timeline_filter', 'type': 'BOOLEAN'},     # Indicates if a user's timeline has already been filtered
                   {'table': 'users', 'col': 'timeline_document', 'type': 'TEXT[]'},        # Document created from relevant tweets
                   {'table': 'topics', 'col': 'document_frequency', 'type': 'FLOAT'},       # Docuemnt frequency
                   {'table': 'users', 'col': 'decision_candid_tfdf_score', 'type': 'FLOAT'}]
    for i in new_columns:
        try:
            json_to_database.make_sql_edit(cur, conn, "ALTER TABLE {} ADD {} {};".format(i['table'], i['col'], i['type']))
            print "Add column {} to table {}.".format(i['col'], i['table'])
        except psycopg2.ProgrammingError:
            conn.rollback()
    new_ind = [{'table': 'users', 'col': 'has_timeline_filter'}]

    # SAMPLING LOOP
    print "\nBegin collection"
    cur_hop = place_savers['cur_hop']
    for ii in range(cur_hop, hop_limits['max_hops']):
        print "\nWorking on collecting hop {} containing {} profiles.".format(ii, len(place_savers['cur_user_list']))
        # GET PROFILE INFORMATION
        if ii < 1:
            _get_profiles_wrapper(cur=cur, conn=conn, user_list=place_savers['cur_user_list'], proxies=proxies, auth=OAUTH, list_type='screen_name', hop=ii)
        else:
            _get_profiles_wrapper(cur=cur, conn=conn, user_list=place_savers['cur_user_list'], proxies=proxies, auth=OAUTH, list_type='user_id', hop=ii)
        growth_params['h{}_users.json'.format(ii)] = set(place_savers['cur_user_list'])
        breadth_first_sampling.save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=ii)

        # GET TIMELINES
        cur.execute("SELECT user_id FROM users WHERE (has_timeline IS NULL) AND (expand_user = TRUE OR expand_user IS NULL) AND (khop = {});".format(ii))
        uids = cur.fetchall()
        for j in uids:
            # Get timeline
            _get_timeline_wrapper(cur=cur, conn=conn, user_id=j[0], tl_start_date=tl_start_date, proxies=proxies, auth=OAUTH)
        # Filter users by timeline
        _filter_by_timeline(cur=cur, conn=conn, tl_start_date=tl_start_date, tl_end_date=tl_end_date, khop=ii)
        # Create documents from timelines
        candid_tfidf.create_documents(cur, conn, tl_start_date, tl_end_date)
        # Expand relevant seed users
        json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user = TRUE WHERE khop = 0 AND expand_user IS NULL;")

        # GET NEXT SET OF USERS
        if ii < (hop_limits['max_hops'] - 1):
            # USER MENTIONS
            print "\nCHOOSE NEXT SET OF USERS FROM USER MENTIONS"
            new_um = set([])
            cur.execute(cur.mogrify("SELECT DISTINCT tweets.user_mentions FROM tweets INNER JOIN users ON users.user_id=tweets.user_id WHERE (tweets.created_at >= %s AND tweets.created_at <= %s) AND users.expand_user = TRUE AND users.khop = %s AND (tweets.user_mentions IS NOT NULL OR tweets.user_mentions != '{}');", (tl_start_date, tl_end_date, ii)))
            uids = cur.fetchall()
            for t in uids:
                new_um = new_um.union(set(t[0]))
            # Get user mention profiles
            _get_profiles_wrapper(cur=cur, conn=conn, user_list=new_um, proxies=proxies, auth=OAUTH, list_type='user_id', hop=ii+1)
            # Get timelines
            for um in new_um:
                _get_timeline_wrapper(cur=cur, conn=conn, user_id=um, tl_start_date=tl_start_date, proxies=proxies, auth=OAUTH)
            # Filter users by timeline
            _filter_by_timeline(cur=cur, conn=conn, tl_start_date=tl_start_date, tl_end_date=tl_end_date, khop=ii+1)
            # Create documents from timelines
            candid_tfidf.create_documents(cur, conn, tl_start_date, tl_end_date)
            # User mentions who have expand_user = NULL, will be set to TRUE
            for um in new_um:
                json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user = TRUE WHERE expand_user IS NULL AND user_id = {};".format(um))
            growth_params['h{}_user_mentions.json'.format(ii)] = set(new_um)
            breadth_first_sampling.save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=ii)
            del new_um

            # FRIENDS AND FOLLOWERS
            print "\nCHOOSE NEXT SET OF USERS FROM FRIENDS AND FOLLOWERS"
            print "Collect friends"
            cur.execute("SELECT user_id FROM users WHERE expand_user = TRUE AND khop = {} AND friends_count > 0 AND friends_list IS NOT NULL;".format(ii))
            hasfriends = cur.fetchall()
            growth_params['h{}_friends.json'.format(ii)] = set([])
            for u in hasfriends:
                print "\nCollect friends for user {}.".format(u[0])
                friends_list = pyTweet.get_user_friends(user_id=u[0], proxies=proxies, auth=OAUTH, limit=100)
                json_to_database.make_sql_edit(cur, conn, cur.mogrify("UPDATE users SET friends_list = %s WHERE user_id = %s;", (friends_list, u[0])))
                growth_params['h{}_friends.json'.format(ii)].update(set(friends_list))
            breadth_first_sampling.save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=ii)
            print "Collect followers"
            cur.execute("SELECT user_id FROM users WHERE expand_user = TRUE AND khop = {} AND followers_count > 0 AND followers_list IS NOT NULL;".format(ii))
            hasfollowers = cur.fetchall()
            growth_params['h{}_followers.json'] = set([])
            for u in hasfollowers:
                print "\nCollect followers for user {}.".format(u[0])
                followers_list = pyTweet.get_user_followers(user_id=u[0], proxies=proxies, auth=OAUTH, limit=100)
                json_to_database.make_sql_edit(cur, conn, cur.mogrify("UPDATE users SET followers_list = %s WHERE user_id = %s;", (followers_list, u[0])))
                growth_params['h{}_followers.json'].update(set(followers_list))
            breadth_first_sampling.save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=ii)
            print "Get profiles and timelines of friends and followers"
            cur.execute("SELECT user_id, friends_list,followers_list FROM users WHERE expand_user = TRUE AND khop = {} AND (ARRAY_LENGTH(friends_list, 1) > 0 OR ARRAY_LENGTH(followers_list, 1) > 0);".format(ii))
            flist = cur.fetchall()
            ids = set([])
            for f in flist:
                if f is not None:
                    if f[1] is not None:
                        ids.update(f[1])
                    if f[2] is not None:
                        ids.update(f[2])
            ids = list(ids)
            # Get profiles of friends/followers
            _get_profiles_wrapper(cur=cur, conn=conn, user_list=ids, proxies=proxies, auth=OAUTH, list_type='user_id', hop=ii+1)
            for i in range(len(ids)):
                print "\nGet timeline for friend/follower {}: {} out of {}".format(ids[i], i, len(ids))
                _get_timeline_wrapper(cur=cur, conn=conn, user_id=ids[i], tl_start_date=tl_start_date, proxies=proxies, auth=OAUTH)
            # Filter profiles by timeline
            _filter_by_timeline(cur=cur, conn=conn, tl_start_date=tl_start_date, tl_end_date=tl_end_date, khop=ii+1)
            # Create documents from timelines
            candid_tfidf.create_documents(cur, conn, tl_start_date, tl_end_date)
            # Compute CANDID information score, and discriminate users
            for f in flist:
                if (f is not None) and (f[0] is not None):
                    candid_tfidf.compute_candid_score(cur=cur, conn=conn, parent_id=f[0], tl_start_date=tl_start_date, tl_end_date=tl_end_date, threshold_percentile=collection_limits['threshold_percentile'], tf_type=collection_limits['tf_type'])

        # PREPARE FOR NEXT HOP
        place_savers['cur_hop'] = ii + 1
        place_savers['cur_user_list'] = set([])
        cur.execute("SELECT user_id FROM users WHERE khop = {} AND expand_user = TRUE;".format(ii + 1))
        new_profiles = cur.fetchall()
        for np in new_profiles:
            place_savers['cur_user_list'].add(np[0])
        breadth_first_sampling.save_place_savers(user_dir=save_dir['twitter_profiles'], place_savers=place_savers)


##
# Depth first search for cascades
def _add_all_hashtags(cur, conn, user_id, tl_start_date, tl_end_date, khop):
    """
    Add all hashtags from a user to the topic table.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param user_id: Twitter user ID
    :param tl_start_date: Timeline start date, datetime object
    :param tl_end_date: Timeline end date, datetime object
    :param khop: hop count
    :return nAddedHT: Boolean value indicating if hashtag was added
    """
    nAddedHT = 0
    cur.execute(cur.mogrify("SELECT hashtag_entities FROM tweets WHERE (user_id = " + str(user_id) + ") AND (created_at >= %s AND created_at <= %s) AND (hashtag_entities IS NOT NULL AND hashtag_entities != '{}');"), (tl_start_date, tl_end_date))
    ht = cur.fetchall()
    for j in ht:
        for k in j[0]:
            json_to_database.make_sql_edit(cur, conn, "INSERT INTO topics (topic, khop) VALUES ('{}', {});".format(k.lower(), khop))
            nAddedHT += 1
    return nAddedHT

def _find_relevant_users(cur, conn, user_ids):
    """
    This function expands relevant users.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param user_ids: List of Twitter user IDs
    :return: List of Twitter user IDs
    """
    expand_count = 0
    if isinstance(user_ids, set):
        user_ids = list(user_ids)
    users_expanded_by_rule = set([])
    # Do not expand users who have more than 1000 friends+followers
    json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=FALSE WHERE (friends_count+followers_count > 1000) AND (expand_user IS NULL);")
    # Get topic query vector
    topic_queries = []
    cur.execute("SELECT topic FROM topics;")
    for t in cur:
        topic_queries.append("'%{}%' ~* ARRAY_TO_STRING(hashtag_entities, ',', '*')".format(t[0]))
    # Get users/tweets, and expand users with relevant tweets
    for uu in range(len(user_ids)):
        print "\tLabel user {} relevant: expansion {} of {}.".format(user_ids[uu], uu, len(user_ids))
        idx = 0
        r = 20      # Check 20 hashtags at a time
        while (idx+r) < len(topic_queries):
            topic_query = " OR ".join(topic_queries[idx:(idx+r)])
            cur.execute("SELECT COUNT(tweet_id) FROM tweets WHERE (user_id = {}) AND ({});".format(user_ids[uu], topic_query))
            q = cur.fetchone()
            # If user has no tweets
            if q is None:
                continue
            if q[0] > 0:
                json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=TRUE, timeline_is_relevant=TRUE WHERE user_id = {};".format(user_ids[uu]))
                users_expanded_by_rule.add(user_ids[uu])
                expand_count += 1
                break
            idx += r
            if idx < len(topic_queries) < (idx + r):
                topic_query = " OR ".join(topic_queries[idx:])
                cur.execute("SELECT COUNT(tweet_id) FROM tweets WHERE (user_id = {}) AND ({});".format(user_ids[uu], topic_query))
                q = cur.fetchone()
                if q is None:
                    continue
                if q[0] > 0:
                    json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=TRUE, timeline_is_relevant=TRUE WHERE user_id = {};".format(user_ids[uu]))
                    users_expanded_by_rule.add(user_ids[uu])
                    expand_count += 1
                    break
            # Set remaining timeline_is_relevant=NULL to timeline_is_relevant=FALSE
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET timeline_is_relevant=FALSE WHERE (user_id={}) AND (timeline_is_relevant IS NULL);".format(user_ids[uu]))
    print "We are expanding {} users from hashtags.".format(len(users_expanded_by_rule))
    return users_expanded_by_rule

def depth_first_cascade_search(user_seed, tl_start_date, tl_end_date, postgres_params, host, port, save_dir={}, hop_limits={}):
    """
    This funciton builds a network based on users relevant to seed keywords
    Requires that a PostgreSQL database already exist

    :param user_seed: List of user names
    :param tl_start_date: Beginning of date (datetime.date object) of timelines in collection
    :param tl_end_date: End date (datetime.date object) of timelines in collection
    :param postgres_params: Dictionary containing the fields '', ..., required to connect to a database
    :param host:
    :param port:
    :param save_dir: Directory storing sampling place savers and growth parameters
                    EX. save_dir = {'place_saver_filename': 'name of file'}
    :param hop_limits: Specify your graph constrains with the variable hop_limits. Set the maximum number of hops to
                       make a graph with 'max_hops'.
                          EX. hop_limits = {'max_hops': 2}              # Maximum number of hops in graph
    """
    # CHECK PARAMETERS
    print "\nCheck parameters"
    # Timeline start and end dates
    assert (isinstance(tl_start_date, datetime.date) and isinstance(tl_end_date, datetime.date)), "Both tl_start_date and tl_end_date must be datetime.date objects (i.e. tl_start_date = datetime.date(year=2014, month=1, day=1))."
    assert ((tl_end_date - tl_start_date) > datetime.timedelta(0)), "The end date must be later than the start date. Check the assignments of tl_start_date and tl_end_date."
    # Check PostgreSQL parameters
    assert (('dbname' in postgres_params.keys()) and ('user' in postgres_params.keys()) and ('password' in postgres_params.keys())), "Verify the parameters. The possible fields are 'dbname', 'user', 'password', 'host', and 'port'."
    try:
        conn = psycopg2.connect(" ".join(map(lambda x,y: "{}='{}'".format(x,y), postgres_params.keys(),postgres_params.values())))
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.OperationalError:
        print "OperationalError: Check your login credentials.  Make sure the database exists as well."
        return
    # Check hop_limits dictionary
    if 'max_hops' not in hop_limits:
        hop_limits['max_hops'] = 5
        print "\tNo value was specified for hop_limits['max_hops'], the maximin number of hops in graph, so it will be set to {}.".format(hop_limits['max_hops'])
    # Check save_dir dictionary fields, create directories if they do not already exist
    if ('twitter_profiles' not in save_dir.keys()) or (save_dir['twitter_profiles'].strip() == ''):
        save_dir['twitter_profiles'] = os.path.join(os.getcwd(), 'profiles')
        print "\tNo directory was specified for save_dir['twitter_profiles'] so it will be set to {}.".format(save_dir['twitter_profiles'])
    if not os.path.isdir(save_dir['twitter_profiles']):
        print "\tThe directory {} does not exist...creating it now".format(save_dir['twitter_profiles'])
        os.mkdir(save_dir['twitter_profiles'])

    # SET UP SECONDARY PARAMETERS
    # Create proxies dictionary
    proxies = {'http': 'http://%s:%s' % (host, port), 'https': 'http://%s:%s' % (host, port)}
    # Load twitter keys
    twitter_keys = pyTweet.load_twitter_api_key_set()
    # Load place_savers dictionary
    print "\nGetting information of current hop and finished users..."
    place_savers = breadth_first_sampling.load_place_savers(save_dir['twitter_profiles'])
    print "\tThe current hop is {}".format(place_savers['cur_hop'])
    if place_savers['cur_hop'] < 1:
        place_savers['cur_user_list'] = set(user_seed)
    print "\tWe will collect {} users in hop {}".format(len(place_savers['cur_user_list']), place_savers['cur_hop'])
    breadth_first_sampling.save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)
    # Load growth parameters
    growth_params = breadth_first_sampling.load_growth_params(save_dir['twitter_profiles'])

    # API AUTHORIZATION
    print "\nAPI Authorization"
    OAUTH = pyTweet.get_authorization(twitter_keys)

    # CONFIGURE SCHEMA FOR TF-IDF ANALYSIS
    print "\nConfigure database for TF-IDF analysis"
    json_to_database.configure_database_to_build_network(cur, conn)
    new_columns = [{'table': 'users', 'col': 'decision_tfidf', 'type': 'FLOAT'}]    # used
    for i in new_columns:
        try:
            json_to_database.make_sql_edit(cur, conn, "ALTER TABLE {} ADD {} {};".format(i['table'], i['col'], i['type']))
        except psycopg2.ProgrammingError:
            conn.rollback()

    # SAMPLING LOOP
    print "\nBegin collection"
    cur_hop = place_savers['cur_hop']
    for ii in range(cur_hop, hop_limits['max_hops']):
        print "\nWorking on collecting hop {} containing {} profiles.".format(ii, len(place_savers['cur_user_list']))
        if ii < 1:
            _get_profiles_wrapper(cur=cur, conn=conn, user_list=place_savers['cur_user_list'], proxies=proxies, auth=OAUTH, list_type='screen_name', hop=ii)
            # Replace user names in place_savers['cur_user_list'] with user IDs!
            user_id_set = set([])
            for jj in place_savers['cur_user_list']:
                cur.execute("SELECT user_id FROM users WHERE screen_name = '{}';".format(jj))
                user_id_set.add(cur.fetchone()[0])
            place_savers['cur_user_list'] = set(user_id_set)
            del user_id_set
            breadth_first_sampling.save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)
        else:
            _get_profiles_wrapper(cur=cur, conn=conn, user_list=place_savers['cur_user_list'], proxies=proxies, auth=OAUTH, list_type='user_id', hop=ii)
        # Do not expand users who have more than 1000 friends+followers
        json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=FALSE WHERE (friends_count+followers_count > 1000) AND (expand_user IS NULL);")

        # GET TIMELINES
        for jj in place_savers['cur_user_list']:
            _get_timeline_wrapper(cur=cur, conn=conn, user_id=jj, tl_start_date=tl_start_date, proxies=proxies, auth=OAUTH)
        # Add all of the hashtags from the seed of users
        if ii < 1:
            total_ht_h0 = 0
            print "\nAdd all of the hashtags from the seed of users"
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=TRUE WHERE (khop=0) AND (expand_user IS NULL);")
            cur.execute("SELECT DISTINCT user_id FROM users WHERE (khop=0) AND (expand_user IS TRUE);")
            q = cur.fetchall()
            for qq in q:
                nAdd = _add_all_hashtags(cur=cur, conn=conn, user_id=qq[0], tl_start_date=tl_start_date, tl_end_date=tl_end_date, khop=ii)
                total_ht_h0 = total_ht_h0 + nAdd
            print "Added {} hashtags from hop {}.".format(total_ht_h0, ii)
            cur.execute("SELECT COUNT(*) FROM users WHERE (khop=0) AND (expand_user=TRUE);")
            print "Expand {} users from hop {}".format(cur.fetchone()[0], ii)

        # SAVE GRAPH PARAMS
        growth_params['h{}_users.json'.format(ii)] = set(place_savers['cur_user_list'])
        growth_params['h{}_missing.json'.format(ii)] = set([])
        growth_params['h{}_extendTRUE.json'.format(ii)] = set([])
        growth_params['h{}_extendFALSE.json'.format(ii)] = set([])
        growth_params['h{}_extendNULL.json'.format(ii)] = set([])
        for uu in place_savers['cur_user_list']:
            cur.execute("SELECT expand_user FROM users WHERE user_id = {};".format(uu))
            q = cur.fetchone()
            if q is None:
                growth_params['h{}_missing.json'.format(ii)].add(uu)
                continue
            if q[0] is None:
                growth_params['h{}_extendNULL.json'.format(ii)].add(uu)
            elif q[0] is True:
                growth_params['h{}_extendTRUE.json'.format(ii)].add(uu)
            elif q[0] is False:
                growth_params['h{}_extendFALSE.json'.format(ii)].add(uu)
            else:
                print "ERROR in saving growth parameters! Invalid data type..."
                continue
        breadth_first_sampling.save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=ii)

        # GET NEXT SET OF USERS
        if ii < (hop_limits['max_hops'] - 1):
            print "\nCHOOSE NEXT SET OF USERS FROM USER MENTIONS"
            new_um = set([])
            # Add user mentions to next hop
            for jj in place_savers['cur_user_list']:
                cur.execute(cur.mogrify("SELECT DISTINCT tweets.user_mentions FROM tweets INNER JOIN users ON users.user_id=tweets.user_id WHERE (users.user_id = %s) AND (tweets.created_at >= %s AND tweets.created_at <= %s) AND (users.expand_user IS TRUE) AND (tweets.user_mentions IS NOT NULL OR tweets.user_mentions != '{}');", (jj, tl_start_date, tl_end_date)))
                uids = cur.fetchall()
                for kk in uids:
                    new_um.update(set(kk[0]))
            print "There are {} user mentions from hop {}".format(len(new_um), ii)
            # Get user mention profiles
            _get_profiles_wrapper(cur=cur, conn=conn, user_list=new_um, proxies=proxies, auth=OAUTH, list_type='user_id', hop=ii+1)
            # Expand, or not, user mentions
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=FALSE WHERE (friends_count+followers_count > 1000) AND (expand_user IS NULL);")
            # Expand remaining user mentions
            growth_params['h{}_um_missing.json'.format(ii)] = set([])
            growth_params['h{}_um_extendTRUE.json'.format(ii)] = set([])
            growth_params['h{}_um_extendFALSE.json'.format(ii)] = set([])
            growth_params['h{}_um_extendNULL.json'.format(ii)] = set([])
            new_um_tracker = set(new_um)
            for uu in new_um_tracker:
                cur.execute("SELECT expand_user FROM users WHERE user_id = {};".format(uu))
                q = cur.fetchone()
                if q is None:
                    new_um.remove(uu)
                    growth_params['h{}_um_missing.json'.format(ii)].add(uu)
                    continue
                if q[0] is False:
                    new_um.remove(uu)
                    growth_params['h{}_um_extendFALSE.json'.format(ii)].add(uu)
                else:
                    json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=TRUE WHERE (user_id = {}) AND (expand_user IS NULL);".format(uu))
            new_um_tracker = set(new_um)
            for uu in new_um_tracker:
                cur.execute("SELECT expand_user FROM users WHERE user_id = {};".format(uu))
                q = cur.fetchone()
                if q is None:
                    # print "this is strange"
                    new_um.remove(uu)
                    growth_params['h{}_um_missing.json'.format(ii)].add(uu)
                    continue
                if q[0] is True:
                    growth_params['h{}_um_extendTRUE.json'.format(ii)].add(uu)
                if q[0] is None:
                    growth_params['h{}_um_extendNULL.json'.format(ii)].add(uu)
                    new_um.remove(uu)
                    print "This is not supposed to happen!!!"
            del new_um_tracker
            assert (len(growth_params['h{}_um_extendNULL.json'.format(ii)]) < 1), "There are user mentions assigned expand_user=NULL!"
            place_savers['next_user_list'].update(new_um)
            breadth_first_sampling.save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)
            breadth_first_sampling.save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=ii)

            print "\nCHOOSE NEXT SET OF USERS FROM FRIENDS AND FOLLOWERS"
            print "Collect friends"
            for jj in place_savers['cur_user_list']:
                cur.execute("SELECT expand_user FROM users WHERE (user_id = {}) AND (expand_user IS TRUE) AND (friends_count > 0) AND (friends_list IS NULL);".format(jj))
                q = cur.fetchone()
                if q is None:
                    continue
                if q[0] is True:
                    print "\tCollect friends for user {}.".format(jj)
                    friends_list = pyTweet.get_user_friends(user_id=jj, proxies=proxies, auth=OAUTH, limit=1000)
                    json_to_database.make_sql_edit(cur, conn, cur.mogrify("UPDATE users SET friends_list = %s WHERE user_id = %s;", (friends_list, jj)))
            print "Collect followers"
            for jj in place_savers['cur_user_list']:
                cur.execute("SELECT expand_user FROM users WHERE (user_id = {}) AND (expand_user IS TRUE) AND (followers_count > 0) AND (followers_list IS NULL);".format(jj))
                q = cur.fetchone()
                if q is None:
                    continue
                if q[0] is True:
                    print "\tCollect followers for user {}.".format(jj)
                    followers_list = pyTweet.get_user_followers(user_id=jj, proxies=proxies, auth=OAUTH, limit=1000)
                    json_to_database.make_sql_edit(cur, conn, cur.mogrify("UPDATE users SET followers_list = %s WHERE user_id = %s;", (followers_list, jj)))
            print "Get profiles and timelines of friends and followers"
            fids = set([])
            for jj in place_savers['cur_user_list']:
                cur.execute("SELECT friends_list,followers_list FROM users WHERE (user_id = {}) AND (expand_user IS TRUE) AND (((friends_list IS NOT NULL) AND (ARRAY_LENGTH(friends_list,1) > 0)) OR ((followers_list IS NOT NULL) AND (ARRAY_LENGTH(followers_list,1) > 0)));".format(jj))
                flist = cur.fetchone()
                if flist is None:
                    continue
                if flist[0] is not None:
                    fids.update(flist[0])
                if flist[1] is not None:
                    fids.update(flist[1])
            print "There are {} friends/followers of hop {}".format(len(fids), ii)
            # Get profiles of friends/followers
            _get_profiles_wrapper(cur=cur, conn=conn, user_list=fids, proxies=proxies, auth=OAUTH, list_type='user_id', hop=ii+1)
            # Filter with high degree rule and get timelines
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=FALSE WHERE (friends_count+followers_count > 1000) AND (expand_user IS NULL);")
            # Remove expand_user=FALSE from friend/follower list
            growth_params['h{}_frfo_missing.json'.format(ii)] = set([])
            growth_params['h{}_frfo_extendFALSE.json'.format(ii)] = set([])
            jj_list = list(fids)
            for jj in jj_list:
                cur.execute("SELECT expand_user,has_timeline FROM users WHERE user_id = {};".format(jj))
                q = cur.fetchone()
                if q is None:
                    growth_params['h{}_frfo_missing.json'.format(ii)].add(jj)
                    fids.remove(jj)
                    continue
                if q[0] is False:
                    fids.remove(jj)
                    growth_params['h{}_frfo_extendFALSE.json'.format(ii)].add(jj)
                    continue
                if (q[0] is not False) and (q[1] is None):
                    _get_timeline_wrapper(cur=cur, conn=conn, user_id=jj, tl_start_date=tl_start_date, proxies=proxies, auth=OAUTH)
            del jj_list
            # Find the most similar friends/followers, and expand the top 5%
            original_frfo_set = candid_tfidf.find_most_similar_followers(cur=cur, conn=conn, tl_start_date=tl_start_date, tl_end_date=tl_end_date, user_ids=fids, prev_users=place_savers['cur_user_list'])
            growth_params['h{}_frfo_extendTRUE.json'.format(ii)] = set(fids)
            growth_params['h{}_frfo_extendNULL.json'.format(ii)] = set(original_frfo_set.difference(fids))
            del original_frfo_set
            breadth_first_sampling.save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=ii)
            place_savers['next_user_list'].update(fids)
            breadth_first_sampling.save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)

            if ii > 0:
                print "\nFIND USERS WITH AT LEAST ONE HASHTAG IN COMMON WITH TOPICS"
                new_relevant_users = _find_relevant_users(cur=cur, conn=conn, user_ids=growth_params['h{}_frfo_extendNULL.json'.format(ii)])
                growth_params['h{}_relevant_extendTRUE.json'.format(ii)] = set(new_relevant_users)
                breadth_first_sampling.save_growth_params(user_dir=save_dir['twitter_profiles'], growth_obj=growth_params, cur_hop=ii)
                place_savers['next_user_list'].update(new_relevant_users)
                breadth_first_sampling.save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)

        # PREPARE FOR NEXT HOP
        place_savers['cur_hop'] += 1
        place_savers['cur_user_list'] = set(place_savers['next_user_list'])
        place_savers['next_user_list'] = set([])
        breadth_first_sampling.save_place_savers(user_dir=save_dir['twitter_profiles'], place_saver_obj=place_savers)


