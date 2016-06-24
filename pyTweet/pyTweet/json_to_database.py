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
Date: September 19, 2014
Installation: Python 2.7 on Windows 7

This script prepares a PostgreSQL database to store Twitter graph data. It then loads both profile and timeline .JSON
files, directly from the Official Twitter API, into the new database.
"""


import os, re, csv, uuid, ujson as jsn, datetime, sys, itertools

from dateutil.parser import parser

import build_network, pyTweet

import psycopg2, psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


##
#  FUNCTIONS FOR PREPARING A DATABASE
def _create_tables(cur, conn):
    """
    This function creates all the tables and indexes for a pyTweet PostgreSQL database. The database must exist, but
    not necessarily be configured, before using this function.

    :param cur: Cursor to database
    :param conn: Connection to database
    """
    print 'Does the table users exist?'
    try:
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'users';")
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'users';")
    q = cur.fetchone()
    if q is not None:
        print "\tYes"
    else:
        print "\tThe table 'users' does not exist"
        users_sql = '''
        CREATE TABLE users
        (
        user_id bigint PRIMARY KEY,
        user_name text,
        screen_name text,
        location text,
        friends_list bigint[],
        followers_list bigint[],
        profile_background_image_url text,
        profile_image_url text,
        profile_url text,
        time_zone text,
        date_of_collection TIMESTAMP WITH TIME ZONE,
        khop int,
        geo_enabled boolean DEFAULT FALSE,
        profile_language character varying(10),
        friends_count bigint,
        followers_count bigint,
        utc_offset text
        );
        '''
        make_sql_edit(cur, conn, users_sql)
        print "The table 'users' was created."
    # tweets table
    print "Does the table 'tweets' exist?"
    try:
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'tweets';")
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'tweets';")
    q = cur.fetchone()
    if q is not None:
        print "\tYes"
    else:
        print "\tThe table 'tweets' does not exist"
        tweets_sql = '''
        CREATE TABLE tweets
        (
        tweet_id bigint PRIMARY KEY,
        user_id bigint NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE,
        tweet text NOT NULL,
        user_mentions bigint[],
        hashtag_entities text[],
        url_entities text[],
        in_reply_to_status_id bigint,
        in_reply_to_user_id bigint,
        latitude double precision,
        longitude double precision,
        retweet_count int,
        country text,
        place_full_name text,
        place_type text,
        place_url text,
        favorite_count bigint,
        date_of_collection TIMESTAMP WITH TIME ZONE
        );
        '''
        make_sql_edit(cur, conn, tweets_sql)
        print "The table 'tweets' was created."
    # graph table
    print "Does the table 'graph' exist?"
    try:
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'graph';")
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'graph';")
    q = cur.fetchone()
    if q is not None:
        print "\tYes"
    else:
        print "\tThe table 'graph' does not exist"
        graph_sql = '''
        CREATE TABLE graph
        (
        edge_id SERIAL PRIMARY KEY,
        from_id bigint NOT NULL REFERENCES users,
        to_id bigint NOT NULL REFERENCES users,
        edge_type smallint NOT NULL,
        tweet_id bigint,
        hashtag text
        );
        '''
        make_sql_edit(cur, conn, graph_sql)
        print "The table 'graph' was created."
    # Create indices
    index_cmd = '''
    DROP INDEX IF EXISTS users_khop_idx;
    DROP INDEX IF EXISTS users_friends_count_idx;
    DROP INDEX IF EXISTS users_followers_count_idx;
    DROP INDEX IF EXISTS users_date_of_collection_idx;
    CREATE INDEX ON users (khop);
    CREATE INDEX ON users (friends_count);
    CREATE INDEX ON users (followers_count);
    CREATE INDEX ON users (date_of_collection);

    DROP INDEX IF EXISTS tweets_user_id_idx;
    DROP INDEX IF EXISTS tweets_in_reply_to_user_id_idx;
    DROP INDEX IF EXISTS tweets_in_reply_to_status_id_idx;
    DROP INDEX IF EXISTS tweets_retweet_count_idx;
    DROP INDEX IF EXISTS tweets_created_at_idx;
    DROP INDEX IF EXISTS tweets_date_of_collection_idx;
    CREATE INDEX ON tweets (user_id);
    CREATE INDEX ON tweets (in_reply_to_user_id);
    CREATE INDEX ON tweets (in_reply_to_status_id);
    CREATE INDEX ON tweets (retweet_count);
    CREATE INDEX ON tweets (created_at);
    CREATE INDEX ON tweets (date_of_collection);

    DROP INDEX IF EXISTS graph_from_id_idx;
    DROP INDEX IF EXISTS graph_to_id_idx;
    DROP INDEX IF EXISTS graph_tweet_id_idx;
    DROP INDEX IF EXISTS graph_edge_type_idx;
    CREATE INDEX ON graph (from_id);
    CREATE INDEX ON graph (to_id);
    CREATE INDEX ON graph (tweet_id);
    CREATE INDEX ON graph (edge_type);
    '''
    make_sql_edit(cur, conn, index_cmd)

def clear_tables(postgres_params):
    """
    This function clears tables from the database.

    :param postgres_params: A dictionary object containing the keys 'dbname', 'user', 'password', 'host', and 'port'.
    """
    # Check PostgreSQL parameters
    assert (('dbname' in postgres_params.keys()) and ('user' in postgres_params.keys()) and ('password' in postgres_params.keys())), "Verify the parameters. The possible fields are 'dbname', 'user', 'password', 'host', and 'port'."
    try:
        conn = psycopg2.connect(" ".join(map(lambda x,y: "{}='{}'".format(x,y), postgres_params.keys(),postgres_params.values())))
    except psycopg2.OperationalError:
        print "OperationalError: Check your login credentials.  Make sure the database exists as well."
        return
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Clear tables
    for table in ['tweets', 'users', 'graph']:
        print 'Clearing table: {}'.format(table)
        make_sql_edit(cur, conn, "delete from {};".format(table))
    cur.close()
    conn.close()

def prepare_graph_database(postgres_params):
    """
    This function prepares and clears the graph database.

    :param postgres_params: A dictionary object containing the keys 'dbname', 'user', 'password', 'host', and 'port'.
    """
    # Check PostgreSQL parameters
    assert (('dbname' in postgres_params.keys()) and ('user' in postgres_params.keys()) and ('password' in postgres_params.keys())), "Verify the parameters. The possible fields are 'dbname', 'user', 'password', 'host', and 'port'."
    try:
        conn = psycopg2.connect(" ".join(map(lambda x,y: "{}='{}'".format(x,y), postgres_params.keys(),postgres_params.values())))
    except psycopg2.OperationalError:
        print "OperationalError: Check your login credentials.  Make sure the database exists as well."
        return
    # Create cursor
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Create tables and indexes
    _create_tables(cur=cur, conn=conn)

##
# Configure database for sampling
def configure_database_to_build_network(cur, conn):
    """
    Configure the standard database for TF-IDF analysis.  This involves creating the topics table and adding columns
    to the users table.

    :param cur: Cursor to database
    :param conn: Connection to database
    """
    # The has_tl column indicates whether a timeline has been collected. FYI timeline may not have any tweets
    new_columns = [{'table': 'users', 'col': 'has_timeline', 'type': 'BOOLEAN'},
                   {'table': 'users', 'col': 'expand_user', 'type': 'BOOLEAN'},
                   {'table': 'users', 'col': 'timeline_is_relevant', 'type': 'BOOLEAN'},
                   {'table': 'users', 'col': 'has_timeline_filter', 'type': 'BOOLEAN'}]      #
    for ii in new_columns:
        try:
            make_sql_edit(cur, conn, "ALTER TABLE {} ADD {} {};".format(ii['table'], ii['col'], ii['type']))
        except psycopg2.ProgrammingError:
            conn.rollback()
        make_sql_edit(cur, conn, "DROP INDEX IF EXISTS {}_{}_idx;".format(ii['table'], ii['col']))
        make_sql_edit(cur, conn, "CREATE INDEX ON {} ({});".format(ii['table'], ii['col']))
    # Create topic table
    print 'Does the table topics exist?'
    try:
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'topics';")
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'topics';")
    q = cur.fetchone()
    if q is not None:
        print "\tYes"
    else:
        print '\tThe table topics does not exist'
        users_sql = '''
        CREATE TABLE topics
        (
        topic_id SERIAL PRIMARY KEY,
        topic text UNIQUE,
        khop INT
        );
        '''
        make_sql_edit(cur, conn, users_sql)
        print '\tThe table topics was created.'
    # Create deleted profile topic table
    print 'Does the table lost_profiles exist?'
    try:
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'lost_profiles';")
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'lost_profiles';")
    q = cur.fetchone()
    if q is not None:
        print "\tYes"
    else:
        print '\tThe table lost_profiles does not exist'
        users_sql = '''
        CREATE TABLE lost_profiles
        (
        profile_id SERIAL PRIMARY KEY,
        screen_name text UNIQUE,
        user_id bigint UNIQUE
        );
        '''
        try:
            make_sql_edit(cur, conn, users_sql)
            print '\tThe table lost_profiles was created.'
        except psycopg2.ProgrammingError:
            conn.rollback()
            pass
        index_cmd = '''
        DROP INDEX IF EXISTS lost_profiles_user_id_idx;
        CREATE INDEX ON lost_profiles(user_id);
        '''
        make_sql_edit(cur, conn, index_cmd)

def make_sql_edit(cur, conn, cmd):
    """
    Run an SQL edit and catch potential errors.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param cmd: SQL command in string format
    """
    try:
        cur.execute(cmd)
        conn.commit()
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute(cmd)
        conn.commit()
    except psycopg2.IntegrityError:
        conn.rollback()





##
#  FUNCTIONS FOR FORMATING DATA
def _convert_created_at(dstr):
    """
    This function converts the string returned from a tweet's 'created_at' field to a datetime object. It assumes that
    all dstr are in UTC timezone (which they are according to the API).

    :param dstr: a tweet's 'created_at' string, ex. 'Wed Apr 22 03:39:06 +0000 2015'
    :return dt: datetime object
    """
    if isinstance(dstr, datetime.datetime):
        return dstr
    else:
        p = parser()
        return p.parse(dstr)

def _parse_user_mention_entities(data):
    """
    This function parses the user mention entity. It adds new users to the database. A list of user ids is returned.

    :param data: dictionary object of user mentions directly from Official Twitter API
    :return user_mentions: list of user mention ids
    """
    if (data is None) or (data == []):
        return []
    # User mentions is an array list of user ids
    user_mentions = []
    for ii in range(0, len(data)):
        user_mentions.append(data[ii]['id'])
    return user_mentions

def _parse_url_entities(data):
    """
    This function parses url entities

    :param data: tldata[twt]['entities']['urls']
    :return formatted url entities
    """
    if (data is None) or (data == []):
        return ""
    urls =[]
    for ii in range(len(data)):
        try:
            urls.append(data[ii]['expanded_url'])
        except IndexError:
            continue
    return urls

def _parse_hashtag_entities(data):
    """
    This function parses hashtag entities

    :param data: tldata[twt]['entities']['hashtags']
    :return formatted hashtag entities
    """
    if (data is None) or (data == []):
        return ""
    htags = []
    for ii in range(0, len(data)):
        try:
            htags.append(data[ii]['text'])
        except IndexError:
            continue
    return htags


##
#  THE FOLLOWING FUNCTIONS POPULATE A DATABASE
def _lookup_max_edge_id(cur):
    """
    Look up graph edge id.

    :param cur: Cursor to database
    :return maximum edge id
    """
    cur.execute('SELECT MAX(edge_id) FROM graph;')
    ii = cur.fetchall()
    if (ii[0][0] == []) or (ii[0][0] is None):
        return 0
    else:
        return ii[0][0]

def add_user(userdata, cur, conn):
    """
    This function adds the author of a timeline to the users table if they are not already in there.

    :param userdata: user data in a dictionary
    :param cur: Cursor to database
    :param conn: Connection to database
    :return boolean: True if user was successfully added to the table
    """
    # Has the user been added to the table?
    try:
        cur.execute('SELECT user_id FROM users WHERE user_id= ' + str(userdata['id']) + ';')
        res = cur.fetchall()
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute('SELECT user_id FROM users WHERE user_id= ' + str(userdata['id']) + ';')
        res = cur.fetchall()
    if res != []: return (cur, conn, True)
    # Parse userdata
    good_data = {}
    good_data['user_id'] = str(userdata['id'])
    if 'name' in userdata.keys():
        good_data['user_name'] = userdata['name']
    if 'screen_name' in userdata.keys():
        good_data['screen_name'] = userdata['screen_name']
    if ('location' in userdata.keys()) and (userdata['location'] is not None) and (userdata['location'] != ''):
        good_data['location'] = userdata['location']
    if 'profile_background_image_url' in userdata.keys():
        good_data['profile_background_image_url'] = userdata['profile_background_image_url']
    if 'profile_image_url' in userdata.keys():
        good_data['profile_image_url'] = userdata['profile_image_url']
    if 'url' in userdata.keys():
        good_data['profile_url'] = userdata['url']
    if 'time_zone' in userdata.keys():
        good_data['time_zone'] = userdata['time_zone']
    if 'utc_offsest' in userdata.keys():
        good_data['utc_offset'] = userdata['utc_offset']
    if ('geo_enabled' in userdata.keys()) and (userdata['geo_enabled']):
        good_data['geo_enabled'] = 'TRUE'
    if 'lang' in userdata.keys():
        good_data['profile_language'] = userdata['lang']
    if ('friends_list' in userdata.keys()) and (userdata['friends_list'] != []):
        good_data['friends_list'] = userdata['friends_list']
        if good_data['friends_list'] == 'ARRAY[]': del good_data['friends_list']
    if ('followers_list' in userdata.keys()) and (userdata['followers_list'] != []):
        good_data['followers_list'] = userdata['followers_list']
        if good_data['followers_list'] == 'ARRAY[]': del good_data['followers_list']
    if 'url' in userdata.keys():
        good_data['profile_url'] = userdata['url']
    if ('DOC' in userdata.keys()) and (userdata['DOC'] is not None) and (userdata['DOC'] != ''):
        good_data['date_of_collection'] = userdata['DOC']
    if 'khop' in userdata.keys():
        good_data['khop'] = str(userdata['khop'])
    if 'friends_count' in userdata.keys():
        good_data['friends_count'] = str(userdata['friends_count'])
    if 'followers_count' in userdata.keys():
        good_data['followers_count'] = str(userdata['followers_count'])
    # Insert into database
    cmd = cur.mogrify("INSERT INTO users (" + ", ".join(good_data.keys()) + ") VALUES (" + ", ".join(['%s'] * len(good_data)) + ");", tuple( good_data.values()))
    try:
        cur.execute(cmd)
        conn.commit()
        return True
    except psycopg2.InternalError:
        conn.rollback()
        cur.execute(cmd)
        conn.commit()
        return True
    except psycopg2.IntegrityError:
        return False
    except psycopg2.ProgrammingError:
        return False
    except psycopg2.DataError:
        return False

def load_user_information(postgres_params, user_dir=os.curdir):
    """
    Load profile .json files into database.

    :param postgres_params: A dictionary object containing the keys 'dbname', 'user', 'password', 'host', and 'port'.
    :param user_dir (Default is current working directory): Directory of profile JSONs
    """
    # Check parameters and connect to database
    assert (('dbname' in postgres_params.keys()) and ('user' in postgres_params.keys()) and ('password' in postgres_params.keys())), "Verify the parameters. The possible fields are 'dbname', 'user', 'password', 'host', and 'port'."
    try:
        conn = psycopg2.connect(" ".join(map(lambda x,y: "{}='{}'".format(x,y), postgres_params.keys(),postgres_params.values())))
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.OperationalError:
        print "OperationalError: Check your login credentials.  Make sure the database exists as well."
        return
    # Load user profile data
    user_jsons = os.listdir(user_dir)
    user_jsons = filter(lambda k: re.match('.+\.json', k), user_jsons)
    user_jsons = [os.path.join(user_dir, j) for j in user_jsons]
    # Fill in users table
    for idx,file in enumerate(user_jsons):
        if idx % 100 == 0:
            print '\n{} of {}: JSON {}'.format(idx+1, len(user_jsons), file)
        # Read file
        try:
            if os.path.getsize(file) == 0:
                continue    # Skip 0 byte files
            jfid = open(file)
            userdata = jsn.load(jfid)
            jfid.close()
        except:
            continue
        # Is this a profile metadata JSON?
        try:
            userdata['id']
            if ('DOC' not in userdata.keys()) or (userdata['DOC'] is None) or (userdata['DOC'] == ''):
                userdata['DOC'] = datetime.datetime.fromtimestamp(os.path.getmtime(file))
        except (TypeError, KeyError):
            print "\tThe key 'id' must be present in the JSON {}. Are you sure that this is Twitter profile metadata?".format(file)
            continue
        # Parse .json and add to user to database
        try:
            isInTable = add_user(userdata=userdata, cur=cur, conn=conn)
        except KeyError:
            print "KeyError adding file {}. Are you sure it's Twitter profile metadata?".format(file)
    cur.close()
    conn.close()

def add_timeline(cur, conn, tldata):
    """
    This function adds a timeline to the database

    :param cur: Cursor to database
    :param conn: Connection to database
    :param tldata: Timeline dictionary object from Official Twitter API
    :return tweetAdded: Boolean value that's true when a tweet has been added
    """
    tweetAdded = False
    for twt in range(len(tldata)):
        # Is tweet already in table?
        try:
            cur.execute('SELECT tweet_id FROM tweets WHERE tweet_id=' + str(tldata[twt]['id']) + ';')
            res = cur.fetchall()
        except psycopg2.InternalError:
            conn.rollback()
            cur.execute('SELECT tweet_id FROM tweets WHERE tweet_id=' + str(tldata[twt]['id']) + ';')
            res = cur.fetchall()
        if res != []:
            continue
        # Add author of twt to users table
        add_user(userdata=tldata[twt]['user'], cur=cur, conn=conn)
        # Parse tweet metadata
        good_data = {}
        good_data['tweet_id'] = str(tldata[twt]['id'])
        good_data['user_id'] = str(tldata[twt]['user']['id'])
        good_data['created_at'] = _convert_created_at(tldata[twt]['created_at'])
        good_data['tweet'] = tldata[twt]['text']
        if ('place' in tldata[twt]) and (tldata[twt]['place'] is not None):
            if 'country' in tldata[twt]['place']:
                good_data['country'] = tldata[twt]['place']['country']
            if 'place_type' in tldata[twt]['place']:
                good_data['place_type'] = tldata[twt]['place']['place_type']
            if 'full_name' in tldata[twt]['place']:
                good_data['place_full_name'] = tldata[twt]['place']['full_name']
            if 'place_url' in tldata[twt]['place']:
                good_data['place_url'] = tldata[twt]['place']['url']
        if ('geo' in tldata[twt]) and (tldata[twt]['geo'] is not None) and ('coordinates' in tldata[twt]['geo']):
            good_data['longitude'] = str(tldata[twt]['geo']['coordinates'][0])
            good_data['latitude'] = str(tldata[twt]['geo']['coordinates'][1])
        if tldata[twt]['entities']['user_mentions'] != []:
            um_list = _parse_user_mention_entities(tldata[twt]['entities']['user_mentions'])
            good_data['user_mentions'] = um_list
            if um_list == []: del good_data['user_mentions']
        if tldata[twt]['entities']['hashtags'] != []:
            good_data['hashtag_entities'] = _parse_hashtag_entities(tldata[twt]['entities']['hashtags'])
            if good_data['hashtag_entities'] == []: del good_data['hashtag_entities']
        if (tldata[twt]['entities']['urls'] != []) and (tldata[twt]['entities']['urls'] is not None):
            good_data['url_entities'] = _parse_url_entities(tldata[twt]['entities']['urls'])
            if good_data['url_entities'] == []: del good_data['url_entities']
        if tldata[twt]['in_reply_to_status_id'] is not None:
            good_data['in_reply_to_status_id'] = str(tldata[twt]['in_reply_to_status_id'])
        if tldata[twt]['in_reply_to_user_id'] is not None:
            good_data['in_reply_to_user_id'] = str(tldata[twt]['in_reply_to_user_id'])
        if 'retweet_count' in tldata[twt]:
            good_data['retweet_count'] = str(tldata[twt]['retweet_count'])
        if 'favorite_count' in tldata[twt]:
            good_data['favorite_count'] = str(tldata[twt]['favorite_count'])
        if 'DOC' in tldata[twt]:
            if isinstance(tldata[twt]['DOC'], unicode):
                dt = datetime.datetime.strptime(tldata[twt]['DOC'].strip(), "%m-%d-%Y %H:%M:%S")
                good_data['date_of_collection'] = dt
            else:
                good_data['date_of_collection'] = tldata[twt]['DOC']
        # Write tweet to database
        cmd = cur.mogrify("INSERT INTO tweets (" + ", ".join(good_data.keys()) + ") VALUES (" + ", ".join(['%s'] * len(good_data)) + ");", tuple( good_data.values()))
        try:
            cur.execute(cmd)
            conn.commit()
            tweetAdded = True
        except psycopg2.InternalError:
            conn.rollback()
            cur.execute(cmd)
            conn.commit()
            tweetAdded = True
        except psycopg2.IntegrityError:
            conn.rollback()
            tweetAdded = False
        # except psycopg2.ProgrammingError:
        #     conn.rollback()
        #     tweetAdded = False
        # except psycopg2.DataError:
        #     conn.rollback()
        #     tweetAdded = False
        if tweetAdded and (twt % 1000 == 0):
            print 'Tweet {} of {}: The tweet {} has been successfully added.'.format(twt, len(tldata), good_data['tweet_id'])
    return tweetAdded

def load_timelines_information(postgres_params, timeline_dir=os.curdir):
    """
    Load timeline .json files into database.

    :param postgres_params: A dictionary object containing the keys 'dbname', 'user', 'password', 'host', and 'port'.
    :param timeline_dir: Directory of timeline JSON files. Default is current directory
    """
    # Check parameters and connect to database
    assert (('dbname' in postgres_params.keys()) and ('user' in postgres_params.keys()) and ('password' in postgres_params.keys())), "Verify the parameters. The possible fields are 'dbname', 'user', 'password', 'host', and 'port'."
    try:
        conn = psycopg2.connect(" ".join(map(lambda x,y: "{}='{}'".format(x,y), postgres_params.keys(),postgres_params.values())))
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.OperationalError:
        print "OperationalError: Check your login credentials.  Make sure the database exists as well."
        return
    # Load user profile file names
    tl_jsons = os.listdir(timeline_dir)
    tl_jsons = filter(lambda k: re.match('.+\.json', k), tl_jsons)
    tl_jsons = [os.path.join(timeline_dir, j) for j in tl_jsons]
    # Register the related typecasters globally as soon as Psycopg is imported to uniformly receive all your database input in Unicode.
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
    # Fill in users table
    for idx,j in enumerate(tl_jsons):
        print '\n{} of {}: JSON {}'.format(idx+1, len(tl_jsons), j)
        # Open file and load data
        try:
            if os.path.getsize(j) == 0:
                continue    # Skip 0 byte files
            jfid = open(j)
            tldata = jsn.load(jfid)
            jfid.close()
            if tldata == None:
                continue
        except:
            continue
        if isinstance(tldata, list):
            for t in range(len(tldata)):
                if 'id' not in tldata[t].keys():
                    print "A tweet ID doesn't appear to be in this value of the list. Are you sure it's a Twitter tweet metadata JSON?".format(j)
                    continue
                if ('DOC' not in tldata[t].keys()) or (tldata[t]['DOC'] is None) or (tldata[t]['DOC'] == ''):
                    tldata[t]['DOC'] = datetime.datetime.fromtimestamp(os.path.getmtime(j))
                if ('DOC' not in tldata[t]['user'].keys()) or (tldata[t]['user']['DOC'] is None) or (tldata[t]['user']['DOC'] == ''):
                    tldata[t]['user']['DOC'] = datetime.datetime.fromtimestamp(os.path.getmtime(j))
        if isinstance(tldata, dict):
            if 'id' not in tldata.keys():
                print "A tweet ID doesn't appear to be in the file {}. Are you sure it's a Twitter tweet metadata JSON?".format(j)
                continue
            if 'user' not in tldata.keys():
                print "An author doesn't appear to be in the file {}. Are you sure it's a Twitter tweet metadata JSON?".format(j)
                continue
            if ('DOC' not in tldata.keys()) or (tldata['DOC'] is None) or (tldata['DOC'] == ''):
                tldata['DOC'] = datetime.datetime.fromtimestamp(os.path.getmtime(j))
            if ('DOC' not in tldata['user'].keys()) or (tldata['user']['DOC'] is None) or (tldata['user']['DOC'] == ''):
                tldata['user']['DOC'] = datetime.datetime.fromtimestamp(os.path.getmtime(j))
        try:
            add_timeline(cur=cur, conn=conn, tldata=tldata)
        except KeyError:
            print "\tKey error adding the file {}. Are you sure it's Twitter timeline metadata?".format(j)
    cur.close()
    conn.close()

##
# Create edges in database
def load_edges(postgres_params, edge_types=range(1, 7), add_missing_users=True):
    """
    This function loads edges into the database from friendships (1), followers (2), user mentions (3), replies (4),
    co-mention edge (5), or co-mention-reply edge (6). See pyTweet documentation for more information on edges. This
    function does not add hashtag edges.

    :param postgres_params: A dictionary object containing the keys 'dbname', 'user', 'password', 'host', and 'port'.
    :param edge_types: List of edges type to create - enter numbers.
    :param add_missing_users: Boolean values indicating weather or not to add missing users (aka add user ID to table)
    """
    # Check edge types
    if isinstance(edge_types, int):
        assert (edge_types in range(1, 7)), "The available edge types are 1, 2, 3, 4, 5, and 6. You entered {}".format(edge_types)
    if isinstance(edge_types, list):
        assert (0 < len(edge_types) < 7), "The edge_types parameters specified {} edges while there are only {} available.".format(len(edge_types))
        assert(all(edge_types) in range(1,7)), "Unknown edge types are specified. Note that the available edge types are 1,2,3,4,5,6 when you entered {}.".format(edge_types)
    # Check parameters and connect to database
    assert (('dbname' in postgres_params.keys()) and ('user' in postgres_params.keys()) and ('password' in postgres_params.keys())), "Verify the parameters. The possible fields are 'dbname', 'user', 'password', 'host', and 'port'."
    try:
        conn = psycopg2.connect(" ".join(map(lambda x,y: "{}='{}'".format(x,y), postgres_params.keys(),postgres_params.values())))
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.OperationalError:
        print "OperationalError: Check your login credentials.  Make sure the database exists as well."
        return
    # Register the related typecasters globally as soon as Psycopg is imported to uniformly receive all your database input in Unicode.
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
    # How often to print loading edges
    nPrint = 75000
    # Does the table lost_profiles exist?
    lost_profiles_exist = False
    cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'lost_profiles';")
    q = cur.fetchone()
    if (q != []) and (q is not None):
        lost_profiles_exist = True
    # Define and execute edge operations
    edge_operations = {1: _load_friendship_edges,
                       2: _load_follower_edges,
                       3: _load_mention_edges,
                       4: _load_reply_edges,
                       5: _load_co_mention_edges,
                       6: _load_co_reply_edges}
    if isinstance(edge_types, int):
        edge_operations[edge_types](cur=cur, conn=conn, nPrint=nPrint, has_lost_profiles=lost_profiles_exist, add_missing_users=add_missing_users)
    if isinstance(edge_types, list):
        for et in edge_types:
            edge_operations[et](cur=cur, conn=conn, nPrint=nPrint, has_lost_profiles=lost_profiles_exist, add_missing_users=add_missing_users)
    cur.close()
    conn.close()

def _load_friendship_edges(cur, conn, nPrint, has_lost_profiles=False, add_missing_users=True):
    """
    Load user-user edges based on the field friends_list.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param nPrint: Integer. Print statement per nPrint users with friends_list
    :param has_lost_profiles: Bool. Indicates if the table lost_profiles exists in the database.
    :param add_missing_users: Bool. Indicates if users who appear in the friends_list but not in the users table should
    be added to the user table to form an edge.
    """
    # Clear friendship edges from table
    print "\nClear friendship edges from graph table..."
    make_sql_edit(cur, conn, "DELETE FROM graph WHERE edge_type = 1;")
    # Get user ids that have friends_list
    print "Find users with a friends list, and create their edges..."
    cur.execute('SELECT user_id, friends_list FROM users WHERE friends_list IS NOT NULL;')
    data = cur.fetchall()
    print "There are {} users with friend lists.".format(len(data))
    # Find maximum edge_id
    cur_edge_id = _lookup_max_edge_id(cur) + 1
    # Store edge data in a CSV file
    filename = 'friendship_edges_' + str(uuid.uuid4()) + '.csv'
    edge_csv = open(filename, 'a')
    csv_writer = csv.writer(edge_csv)
    for ii in range(len(data)):
        if ii % nPrint == 0:
            print "\tUser {} of {}: Create friendship edges for the user {}".format(ii, len(data), data[ii][0])
        for jj in data[ii][1]:
            # Is friend in lost profiles table?
            if has_lost_profiles:
                cur.execute("SELECT COUNT(*) FROM lost_profiles WHERE user_id = {};".format(jj))
                if cur.fetchone()[0] > 0:
                    continue
            # Be sure to add friend to database if they are not already there
            if add_missing_users:
                isInTable = add_user(userdata={'id': jj}, cur=cur, conn=conn)
            else:
                cur.execute("SELECT COUNT(*) FROM users WHERE user_id = {};".format(jj))
                if cur.fetchone()[0] < 1:
                    continue
            csv_writer.writerow((cur_edge_id, data[ii][0], jj, 1, -1, ''))
            cur_edge_id += 1
    # Load edges into PostgreSQL
    edge_csv.close()
    csv_f = open(filename, 'r')
    try:
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    except psycopg2.InternalError:
        conn.rollback()
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    csv_f.close()
    os.remove(filename)

def _load_follower_edges(cur, conn, nPrint, has_lost_profiles=False, add_missing_users=True):
    """
    Load user-user edges based on the field followers_list.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param nPrint: Integer. Print statement per nPrint users with followers_list
    :param has_lost_profiles: Bool. Indicates if the table lost_profiles exists in the database.
    :param add_missing_users: Bool. Indicates if users who appear in the followers_list but not in the users table should
    be added to the user table to form an edge.
    """
    # Clear follower edges from table
    print "\nClear follower edges from graph table..."
    make_sql_edit(cur, conn, "DELETE FROM graph WHERE edge_type = 2;")
    # Get user ids that have friends_list
    print "Find users with a followers list..."
    cur.execute('SELECT user_id, followers_list FROM users WHERE followers_list IS NOT NULL;')
    data = cur.fetchall()
    # Find maximum edge_id
    cur_edge_id = _lookup_max_edge_id(cur) + 1
    # Store edge data in a CSV file
    filename = 'follower_edges_' + str(uuid.uuid4()) + '.csv'
    edge_csv = open(filename, 'a')
    csv_writer = csv.writer(edge_csv)
    # Create edges for friendship relationships
    print "There are {} users who have followers lists.".format(len(data))
    for ii in range(len(data)):
        if ii % nPrint == 0:
            print "\tUser {} of {}: Create follower edges for the user {}".format(ii, len(data), data[ii][0])
        for jj in data[ii][1]:
            # Is follower in lost_profiles table?
            if has_lost_profiles:
                cur.execute("SELECT COUNT(*) FROM lost_profiles WHERE user_id = {};".format(jj))
                if cur.fetchone()[0] > 0:
                    continue
            # Be sure to add friend to database if they are not already there
            if add_missing_users:
                isInTable = add_user(userdata={'id': jj}, cur=cur, conn=conn)
            else:
                cur.execute("SELECT COUNT(*) FROM users WHERE user_id = {};".format(jj))
                if cur.fetchone()[0] < 1:
                    continue
            # Create edge
            csv_writer.writerow((cur_edge_id, data[ii][0], jj, 2, -1, ''))
            cur_edge_id += 1
    # Load edges into postgresql
    edge_csv.close()
    sql = "copy graph from stdin csv;"
    csv_f = open(filename, 'r')
    try:
        cur.copy_expert(sql, csv_f)
        conn.commit()
    except psycopg2.InternalError:
        conn.rollback()
        cur.copy_expert(sql, csv_f)
        conn.commit()
    csv_f.close()
    os.remove(filename)

def _load_reply_edges(cur, conn, nPrint, has_lost_profiles=False, add_missing_users=True):
    """
    Load tweet reply edges.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param nPrint: Integer. Print statement per nPrint tweets with replies
    :param has_lost_profiles: Bool. Indicates if the table lost_profiles exists in the database.
    :param add_missing_users: Bool. Indicates if users who appear in the reply field but not in the users table should
    be added to the user table to form an edge.
    """
    # Clear reply edges from table
    print "\nClear reply edges from graph table..."
    make_sql_edit(cur, conn, "DELETE FROM graph WHERE edge_type = 4;")
    # Find maximum edge_id
    cur_edge_id = _lookup_max_edge_id(cur) + 1
    # Store edge data in a CSV file
    save_file = 'reply_edges_' + str(uuid.uuid4()) + '.csv'
    edge_csv = open(save_file, 'a')
    csv_writer = csv.writer(edge_csv)
    # Load data
    print "Select reply tweets..."
    cur.execute("SELECT tweet_id,user_id,in_reply_to_user_id,user_mentions FROM tweets WHERE (in_reply_to_status_id IS NOT NULL) AND (in_reply_to_user_id IS NOT NULL);")
    data = cur.fetchall()
    print "There are {} tweets with replies.".format(len(data))
    for ii in range(len(data)):
        in_reply_to_user_id = data[ii][2]
        if ii % nPrint == 0:
            print "\tCreate reply edge {} of {} for tweet {}.".format(ii, len(data), data[ii][0])
        # Is friend in lost profiles table?
        if has_lost_profiles:
            cur.execute("SELECT COUNT(*) FROM lost_profiles WHERE user_id = {};".format(in_reply_to_user_id))
            if cur.fetchone()[0] > 0:
                continue
        # Be sure to add friend to database if they are not already there
        if add_missing_users:
            isInTable = add_user(userdata={'id': in_reply_to_user_id}, cur=cur, conn=conn)
        cur.execute("SELECT COUNT(*) FROM users WHERE user_id = {};".format(in_reply_to_user_id))
        if cur.fetchone()[0] < 1:
            continue
        csv_writer.writerow((cur_edge_id, data[ii][1], in_reply_to_user_id, 4, data[ii][0], ''))
        cur_edge_id += 1
    # Add edges to database
    edge_csv.close()
    csv_f = open(save_file, 'r')
    try:
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    except psycopg2.InternalError:
        conn.rollback()
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    csv_f.close()
    os.remove(save_file)

def _load_co_mention_edges(cur, conn, nPrint, has_lost_profiles=False, add_missing_users=True):
    """
    Load co-refernce user mention edges (5)

    :param cur: Cursor to database
    :param conn: Connection to database
    :param nPrint: Integer. Print statement per nPrint tweet with more than one user mention
    :param has_lost_profiles: Bool. Indicates if the table lost_profiles exists in the database.
    :param add_missing_users: Bool. Indicates if users who appear in the user_mention field but not in the users table
    should be added to the user table to form an edge.
    """
    # Clear reply edges from table
    print "\nClear co-occurring user mention edges from graph table..."
    make_sql_edit(cur, conn, "DELETE FROM graph WHERE edge_type = 5;")
    # Find maximum edge_id
    cur_edge_id = _lookup_max_edge_id(cur) + 1
    # Store edge data in a CSV file
    save_file = 'co_ref_mention_edges_' + str(uuid.uuid4()) + '.csv'
    edge_csv = open(save_file, 'a')
    csv_writer = csv.writer(edge_csv)
    # Find rows that have nonempy user_mention fields
    print "Select tweets with co-user mentions..."
    cur.execute("SELECT tweet_id, user_mentions FROM tweets WHERE (user_mentions IS NOT NULL) AND (ARRAY_LENGTH(user_mentions, 1) > 1);")
    data = cur.fetchall()
    print "There are {} tweets with more than 1 user mentions.".format(len(data))
    for ii in range(len(data)):
        if ii % nPrint == 0:
            print "\tCreate co-mention edges for tweet {}: {} out of {}".format(data[ii][0], ii, len(data))
        # Create user mention edges
        user_in_table = {}
        for jj in data[ii][1]:
            user_in_table[jj] = False
            # Is friend in lost_profiles table?
            if has_lost_profiles:
                cur.execute("SELECT COUNT(*) FROM lost_profiles WHERE user_id = {};".format(jj))
                if cur.fetchone()[0] > 0:
                    continue
            # Be sure to add user mention to database
            if add_missing_users:
                isInTable = add_user(userdata={'id': jj}, cur=cur, conn=conn)
            # Make sure user is in database
            cur.execute("SELECT COUNT(*) FROM users WHERE user_id = {};".format(jj))
            if cur.fetchone()[0] < 1:
                continue
            user_in_table[jj] = True
        # Create co-reference user mention edges
        if sum(user_in_table.values()) > 1:
            idx = [c for c in itertools.combinations(user_in_table.keys(), 2)]
            for i in idx:
                if user_in_table[i[0]] and user_in_table[i[1]]:
                    csv_writer.writerow((cur_edge_id, i[0], i[1], 5, data[ii][0], ''))
                    cur_edge_id += 1
    # Add edges to database
    edge_csv.close()
    csv_f = open(save_file, 'r')
    try:
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    except psycopg2.InternalError:
        conn.rollback()
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    csv_f.close()
    os.remove(save_file)

def _load_mention_edges(cur, conn, nPrint, has_lost_profiles=False, add_missing_users=True):
    """
    Load user mention edges (3)

    :param cur: Cursor to database
    :param conn: Connection to database
    :param nPrint: Integer. Print statement per nPrint tweets with user mentions
    :param has_lost_profiles: Bool. Indicates if the table lost_profiles exists in the database.
    :param add_missing_users: Bool. Indicates if users who appear in the user_mention field but not in the users table
    should be added to the user table to form an edge.
    """
    # Clear reply edges from table
    print "\nClear user mention edges from graph table..."
    make_sql_edit(cur, conn, "DELETE FROM graph WHERE edge_type = 3;")
    # Find maximum edge_id
    cur_edge_id = _lookup_max_edge_id(cur) + 1
    # Store edge data in a CSV file
    save_file = 'mention_edges_' + str(uuid.uuid4()) + '.csv'
    edge_csv = open(save_file, 'a')
    csv_writer = csv.writer(edge_csv)
    # Find rows that have nonempy user_mention fields
    print "Select tweets with user mentions..."
    cur.execute("SELECT tweet_id, user_id, user_mentions FROM tweets WHERE user_mentions IS NOT NULL;")
    data = cur.fetchall()
    print "There are {} tweets with user mentions.".format(len(data))
    for ii in range(len(data)):
        if ii % nPrint == 0:
            print "\tCreate user mention and co-mention edges for tweet {}: {} out of {}".format(data[ii][0], ii, len(data))
        # Create user mention edges
        user_in_table = {}
        for jj in data[ii][2]:
            user_in_table[jj] = False
            # Is friend in lost_profiles table?
            if has_lost_profiles:
                cur.execute("SELECT COUNT(*) FROM lost_profiles WHERE user_id = {};".format(jj))
                if cur.fetchone()[0] > 0:
                    continue
            # Be sure to add user mention to database
            if add_missing_users:
                isInTable = add_user(userdata={'id': jj}, cur=cur, conn=conn)
            # Make sure user is in database
            cur.execute("SELECT COUNT(*) FROM users WHERE user_id = {};".format(jj))
            if cur.fetchone()[0] < 1:
                continue
            user_in_table[jj] = True
            csv_writer.writerow((cur_edge_id, data[ii][1], jj, 3, data[ii][0], ''))
            cur_edge_id += 1
    # Add edges to database
    edge_csv.close()
    csv_f = open(save_file, 'r')
    try:
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    except psycopg2.InternalError:
        conn.rollback()
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    csv_f.close()
    os.remove(save_file)

def _load_co_reply_edges(cur, conn, nPrint, has_lost_profiles=False, add_missing_users=True):
    """
    Load tweet co-reply edges.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param nPrint: Integer. Print statement per nPrint tweets with user mentions and a reply
    :param has_lost_profiles: Bool. Indicates if the table lost_profiles exists in the database.
    :param add_missing_users: Bool. Indicates if users who appear in the user_mention or reply field but not in the users table
    should be added to the user table to form an edge.
    """
    # Clear reply edges from table
    print "\nClear co-reply edges from graph table..."
    make_sql_edit(cur, conn, "DELETE FROM graph WHERE edge_type = 6;")
    # Find maximum edge_id
    cur_edge_id = _lookup_max_edge_id(cur) + 1
    # Store edge data in a CSV file
    save_file = 'reply_co_mention_edges_' + str(uuid.uuid4()) + '.csv'
    edge_csv = open(save_file, 'a')
    csv_writer = csv.writer(edge_csv)
    # Load data
    print "Select reply tweets..."
    cur.execute("SELECT tweet_id,in_reply_to_user_id,user_mentions FROM tweets WHERE (in_reply_to_status_id IS NOT NULL) AND (in_reply_to_user_id IS NOT NULL) AND (user_mentions IS NOT NULL) AND (ARRAY_LENGTH(user_mentions, 1) > 0);")
    data = cur.fetchall()
    print "There are {} tweets with both replies and at least one user mention.".format(len(data))
    for ii in range(len(data)):
        in_reply_to_user_id = data[ii][1]
        if ii % nPrint == 0:
            print "\tCreate co-reply edge {} of {} for tweet {}.".format(ii, len(data), data[ii][0])
        # Is friend in lost profiles table?
        if has_lost_profiles:
            cur.execute("SELECT COUNT(*) FROM lost_profiles WHERE user_id = {};".format(in_reply_to_user_id))
            if cur.fetchone()[0] > 0:
                continue
        # Be sure to add friend to database if they are not already there
        if add_missing_users:
            isInTable = add_user(userdata={'id': in_reply_to_user_id}, cur=cur, conn=conn)
        cur.execute("SELECT COUNT(*) FROM users WHERE user_id = {};".format(in_reply_to_user_id))
        if cur.fetchone()[0] < 1:
            continue
        # Create co-reply-mention edge
        if (data[ii][2] is not None) and (len(data[ii][2]) > 0):
            for um in data[ii][2]:
                # Is mention in lost profiles table?
                if has_lost_profiles:
                    cur.execute("SELECT COUNT(*) FROM lost_profiles WHERE user_id = {};".format(um))
                    if cur.fetchone()[0] > 0:
                        continue
                # Be sure to add friend to database if they are not already there
                if add_missing_users:
                    isInTable = add_user(userdata={'id': um}, cur=cur, conn=conn)
                # else:
                cur.execute("SELECT COUNT(*) FROM users WHERE user_id = {};".format(um))
                if cur.fetchone()[0] < 1:
                    continue
                csv_writer.writerow((cur_edge_id, data[ii][1], um, 6, data[ii][0], ''))
                cur_edge_id += 1
    # Add edges to database
    edge_csv.close()
    csv_f = open(save_file, 'r')
    try:
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    except psycopg2.InternalError:
        conn.rollback()
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    csv_f.close()
    os.remove(save_file)

def load_hashtag_edges(postgres_params, hashtag):
    """
    This function creates edges between users who use the same hashtag (7).

    :param postgres_params: A dictionary object containing the keys 'dbname', 'user', 'password', 'host', and 'port'.
    :param hashtag: Hashtag phrase, leave out the '#'.  The hashtag search is not case sensitive but it is sensitive to punctuation.
    """
    # Check parameters and connect to database
    assert (('dbname' in postgres_params.keys()) and ('user' in postgres_params.keys()) and ('password' in postgres_params.keys())), "Verify the parameters. The possible fields are 'dbname', 'user', 'password', 'host', and 'port'."
    try:
        conn = psycopg2.connect(" ".join(map(lambda x,y: "{}='{}'".format(x,y), postgres_params.keys(),postgres_params.values())))
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except psycopg2.OperationalError:
        print "OperationalError: Check your login credentials.  Make sure the database exists as well."
        return
    assert isinstance(hashtag, str), "The hashtag parameter must be a string."
    assert (hashtag.strip() != ''), "The hashtag parameter appears to be an empty string: {}".format(hashtag.strip())
    print "Create edges for the hashtag '{}'".format(hashtag)
    # Delete edges related to this hashtag
    make_sql_edit(cur, conn, cur.mogrify("DELETE FROM graph WHERE (edge_type = %s) AND (hashtag ~* %s);", (7, hashtag)))
    # Find edges
    cur.execute(cur.mogrify("SELECT DISTINCT user_id FROM tweets WHERE (hashtag_entities IS NOT NULL) AND (ARRAY_LENGTH(hashtag_entities, 1) > %s) AND (%s ~* ARRAY_TO_STRING(hashtag_entities, ',', '*'));", (0, '%' + hashtag + '%')))
    q = cur.fetchall()
    print "There are {} users who've used the hashtag '{}'".format(len(q), hashtag)
    if len(q) < 2:
        return
    # Find maximum edge_id
    cur_edge_id = _lookup_max_edge_id(cur) + 1
    # Store edge data in a CSV file
    save_file = 'hashtag_edges_' + str(uuid.uuid4()) + '.csv'
    edge_csv = open(save_file, 'a')
    csv_writer = csv.writer(edge_csv)
    # Add edges
    idx = [c for c in itertools.combinations(q, 2)]
    for i in idx:
        csv_writer.writerow((cur_edge_id, i[0][0], i[1][0], 7, -1, hashtag))
        cur_edge_id += 1
    # Add edges to database
    edge_csv.close()
    csv_f = open(save_file, 'r')
    try:
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    except psycopg2.InternalError:
        conn.rollback()
        cur.copy_expert("copy graph from stdin csv;", csv_f)
        conn.commit()
    csv_f.close()
    os.remove(save_file)
    cur.close()
    conn.close()



