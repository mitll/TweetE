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

This script computes various measures of textual similarity between users and their followers. The following functions
are primarly used in depth-first sampling methods.
"""


import datetime, time, math, string, numpy as np, sys
from nltk.corpus import stopwords
import pyTweet, json_to_database

# import psycopg2

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


##
# FUNCTIONS FOR TEXT PROCESSING
def process_word_list(word_list):
    """
    This function processes a list of words from Tweets. It removes punctuation from words and converts them to lower
    case. If the word is a URL it is left alone.

    :param word_list: original list of words
    :return processed_word_list: processed list of words
    """
    sw = stopwords.words('english')
    sw.extend(['', '""', "''", 'rt'])
    processed_word_list = []
    for w in word_list:
        if ('http:' in w) or ('https:' in w):
            processed_word_list.append(w)   # Leave URLs as they are
        else:
            try:
                # Convert text to lowercase and remove punctuation
                word = w.translate(string.maketrans("", ""), string.punctuation).lower()
                # Remove English stopwords
                if word not in sw:
                    processed_word_list.append(word)
            except UnicodeEncodeError:
                pass
    return processed_word_list

def create_documents(cur, conn, tl_start_date, tl_end_date):
    """
    This function creates a 'document' for TF-IDF calculations.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param tl_start_date: datetime object to indicate beginning of time line
    :param tl_end_date: datetime object to indicate the end of a time line
    """
    # Select relevant users that do not already have timeline-documents created
    cur.execute(cur.mogrify("SELECT DISTINCT (tweets.user_id) FROM tweets INNER JOIN users ON users.user_id=tweets.user_id WHERE users.timeline_document IS NULL AND users.has_timeline = TRUE AND (users.expand_user IS NULL OR users.expand_user = TRUE) AND (tweets.created_at >= %s AND tweets.created_at <= %s);", (tl_start_date, tl_end_date)))
    uids = cur.fetchall()
    print "\nCreate documents for {} users".format(len(uids))
    # Create the timeline-documents
    for u in range(len(uids)):
        print "\tCreate document for user {}: {} out of {}".format(uids[u][0], u, len(uids))
        timeline_document = []
        # Grab relevant tweets
        cur.execute(cur.mogrify("SELECT tweet FROM tweets WHERE user_id = %s AND (created_at <= %s AND created_at >= %s);", (uids[u][0], tl_end_date, tl_start_date)))
        for twt in cur:
            timeline_document.extend(twt[0].split(' '))
        # Process each word in timeline: convert to lower case, remove punctuation, remove English stop-words
        timeline_document = process_word_list(timeline_document)
        # Add timeline_document to table
        json_to_database.make_sql_edit(cur, conn, cur.mogrify("UPDATE users SET timeline_document = %s WHERE user_id = %s;", (timeline_document, uids[u][0])))
        if len(timeline_document) < 1:
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET timeline_is_relevant = FALSE WHERE user_id = {};".format(uids[u][0]))


##
# New TF-IDF ANALYSIS
def find_most_similar_followers(cur, conn, tl_start_date, tl_end_date, user_ids, prev_users):
    """
    This function identifies the top 10% of most textually simliar followers to a user

    :param cur: Cursor to database
    :param conn: Connection to database
    :param tl_start_date: datetime object to indicate beginning of time line
    :param tl_end_date: datetime object to indicate the end of a time line
    :param user_ids: set of user IDs
    :param prev_users: hop -1
    """
    if 'sklearn' not in sys.modules.keys():
        import sklearn
    original_user_ids = set(user_ids)
    print "\nFind friends/followers most similar to the previous hop using a TF-IDF transformation."
    print "\tBegin with {} friends and followers for similarity test".format(len(user_ids))
    user_timeline_hash = {}     # hash table for user IDs and indexes in the TF-IDF matrix
    # Create document for khop-1 users
    user_doc = ''
    for jj in prev_users:
        cur.execute(cur.mogrify("SELECT tweets.tweet FROM tweets INNER JOIN users ON users.user_id=tweets.user_id WHERE (users.user_id = %s) AND (users.has_timeline=TRUE) AND (users.expand_user=TRUE) AND (tweets.created_at >= %s AND tweets.created_at <= %s);", (jj, tl_start_date, tl_end_date)))
        for t in cur:
            user_doc += t[0]
    corpus = [user_doc]
    user_timeline_hash[0] = 'prev_users'
    # Create document for all hop users
    idx = 1
    jj_users = list(user_ids)
    for jj in jj_users:
        user_doc = ''
        cur.execute(cur.mogrify("SELECT tweets.tweet FROM tweets INNER JOIN users ON users.user_id=tweets.user_id WHERE (users.user_id=%s) AND (users.has_timeline=TRUE) AND (tweets.created_at >= %s AND tweets.created_at <= %s) AND (users.expand_user IS NOT FALSE);", (jj, tl_start_date, tl_end_date)))
        for t in cur:
            user_doc += t[0]
        if user_doc.strip() != '':
            corpus.append(user_doc)
            user_timeline_hash[idx] = jj
            idx += 1
        else:
            user_ids.remove(jj)
    print "\tCompare previous hop with {} friends and followers".format(len(user_timeline_hash)-1)
    if corpus != ['']:
        # Perform TF-IDF transformation
        # tfidf_vectorizer = sklearn.feature_extraction.text.TfidfVectorizer(min_df=1)
        tfidf_vectorizer = TfidfVectorizer(min_df=1)
        tfidf_matrix = tfidf_vectorizer.fit_transform(corpus)
        # Compute cosine similarity between khop-1 and all other timelines
        score = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix)
        # score = sklearn.metrics.pairwise.cosine_similarity(tfidf_matrix[0:1], tfidf_matrix)
        # Exapand the top 5% of users
        if len(score[0]) < 2:
            return
        threshold = np.percentile(score[0][1:], 80)
        expand_idx = np.where(score[0] >= threshold)[0]
        expand_count = 0
        for k in user_timeline_hash.keys():
            if k < 1:
                continue
            if k in expand_idx:
                expand_count += 1
                json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user=TRUE, decision_tfidf={} WHERE user_id={};".format(score[0][k], user_timeline_hash[k]))
            else:
                user_ids.remove(user_timeline_hash[k])
        print "\tExpand {} friends/followers".format(expand_count)
    return original_user_ids



##
# TF-DF ANALYSIS
def compute_df(cur, conn, tl_start_date, tl_end_date, user_set):
    """
    This function computes the IDF for each term in the topic table. Note that a 'document' is defined as a user's
    timeline between tl_start_date_date and tl_end_date.

    IDF(t) = log_e( # of timelines with the term t / # of timelines in database)

    :param cur: Cursor to database
    :param conn: Connection to database
    :param tl_start_date: datetime object to indicate beginning of time line
    :param tl_end_date: datetime object to indicate the end of a time line
    :param user_set: Subset of users to restrict calculation, list object
    """
    print "\nCompute DF for each topic."
    # Get total number of timelines in database
    a = " OR ".join(['user_id = ' + str(j) for j in user_set])
    cur.execute(cur.mogrify("SELECT COUNT (DISTINCT user_id) FROM tweets WHERE (created_at >= %s AND created_at <= %s) AND ({});".format(a), (tl_start_date, tl_end_date)))
    q = cur.fetchone()
    if (q is None) or (q[0] is None):
        print "WARNING: q or q[0] is None!"
        json_to_database.make_sql_edit(cur, conn, "UPDATE topics SET df = 0.0;")
        return
    total_timelines = float(q[0])
    print "\tThere are {} timelines for this set of friends/followers".format(total_timelines)
    # Case: No timelines
    if total_timelines < 1.0:
        json_to_database.make_sql_edit(cur, conn, "UPDATE topics SET df = 0.0;")
        return
    # Get count of timelines containing topic t, for each topic
    cur.execute("SELECT topic FROM topics;")
    topics = cur.fetchall()
    for t in topics:
        # Count the number of timelines that the topic appears in
        topic_freq = 0.0
        if 'http' in t[0]:
            cur.execute(cur.mogrify("SELECT DISTINCT user_id FROM tweets WHERE ({}) AND ((tweet ~ %s) OR (%s = ANY(url_entities))) AND (created_at >= %s AND created_at <= %s);".format(a), ('%' + t[0] + '%', '%' + t[0] + '%', tl_start_date, tl_end_date)))
        else:
            cur.execute(cur.mogrify("SELECT DISTINCT user_id FROM tweets WHERE ({}) AND ((tweet ~* %s) OR (LOWER(%s) = ANY(hashtag_entities))) AND (created_at >= %s AND created_at <= %s);".format(a), ('\m' + t[0] + '\M', t[0], tl_start_date, tl_end_date)))
        q = cur.fetchall()
        topic_freq += float(len(q))
        # Compute IDF
        df = 0.0
        if topic_freq > 0:
            df = math.log(topic_freq/total_timelines, 10.0)
        json_to_database.make_sql_edit(cur, conn, "UPDATE topics SET document_frequency = {} WHERE topic = '{}';".format(df, t[0]))

def compute_tf(document, term, type='raw'):
    """
    This function computes the raw term-frequency (TF) for a given document and term.

    :param document: array of terms (list object)
    :param term: single term
    :param type: Type of TF calculation to use, default is 'raw'. Other options are 'augmented' and 'boolean'
    :return tf: raw term frequency of term in the document
    """
    assert (type in ['raw', 'augmented', 'boolean']), "The parameter 'type' is not recognized. Please enter 'raw', 'boolean' or 'augmented' as it's value."
    tf = 0.0
    if type == 'raw':
        tf = float(document.count(term))
    if type == 'boolean':
        tf = float(term in document)
    if type == 'augmented':
        tf = 0.5 + ((0.5 * float(document.count(term))) / float(max([document.count(x) for x in document])))
    return tf

def compute_tfdf_score(cur, user_id, tf_type):
    """
    This function computes the TF-DF score for a user's timeline.

    :param cur: Cursor to database
    :param user_id: Twitter user ID
    :param tf_type: term-fequency calculation: 'raw', 'augmented', or 'boolean'
    """
    score = 0.0
    # Get timeline_document from user
    cur.execute("SELECT timeline_document FROM users WHERE user_id = {};".format(user_id))
    tl_doc = cur.fetchone()
    # tl_doc = cur.fetchone()[0]
    if (tl_doc is None) or (tl_doc[0] is None) or (len(tl_doc[0]) < 1):
        return score
    # Iterate over all topics to compute the final score
    cur.execute("SELECT topic, document_frequency FROM topics;")
    for t in cur:
        # Compute TF
        tf = compute_tf(document=tl_doc[0], term=t[0], type=tf_type)
        # Compute TF-IDF
        score += tf * t[1]
    return score

def find_top_scores_to_expand(cur, conn, user_id_list, threshold_percentile, tf_type):
    """
    This function returns the top users from user_id_list.

    :param cur: Cursor to database
    :param conn: Connection to database
    :param user_id_list: List of Twitter user IDs
    :param threshold_percentile: Percent of top scores to expand, value between [0, 1]
    :param tf_type:
    """
    assert (0 <= threshold_percentile <= 1), "The parameter 'threshold_percentile' must fall in the range [0, 1]."
    # Compute the CANDID scores
    scores = []
    print "Compute TF-DF scores"
    for f in user_id_list:
        scores.append(compute_tfdf_score(cur=cur,  user_id=f, tf_type=tf_type)/float(len(user_id_list)))
    # Choose top % of friends to expand
    threshold_value = np.percentile(scores, threshold_percentile*100)
    for i in range(len(scores)):
        json_to_database.make_sql_edit(cur, conn, "UPDATE users SET decision_candid_tfdf_score = {} WHERE user_id = {};".format(scores[i], user_id_list[i]))
        if scores[i] >= threshold_value:
            json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user = TRUE WHERE user_id = {} AND expand_user IS NULL;".format(scores[i], user_id_list[i]))
        # else:
        #     json_to_database.make_sql_edit(cur, conn, "UPDATE users SET expand_user = FALSE WHERE user_id = {} AND expand_user IS NULL;".format(scores[i], user_id_list[i]))

def compute_candid_score(cur, conn, parent_id, tl_start_date, tl_end_date, threshold_percentile=0.5, tf_type='raw'):
    """
    This function computes the CANDID TF-IDF score for each user.

    CANDID_TF-DF = TF-DF <explain computation here>

    :param cur: Cursor to database
    :param conn: connection to database
    :param parent_id: user ID of parent node (see calculation description)
    :param tl_start_date: datetime format
    :param tl_end_date: datetime format
    :param threshold_percentile: Everying above this threshold is kept, must fall in the interval [0,1]. The default value is 0.5
    :param tf_type: Type of term-frequncy calculation to employ, default is 'raw'. Other options are 'augmented' and 'boolean'
    """
    assert (0 <= threshold_percentile <= 1), "The threshold_percentile must fall within [0,1]. You entered {}.".format(threshold_percentile)
    assert (tf_type in ['raw', 'augmented', 'boolean']), "The tf_type must be either 'raw', augmented', or 'boolean'. You entered {}.".format(tf_type)
    # Create documents
    create_documents(cur=cur, conn=conn, tl_start_date=tl_start_date, tl_end_date=tl_end_date)
    # Check if parent's extend status
    cur.execute("SELECT expand_user FROM users WHERE user_id = {};".format(parent_id))
    if cur.fetchone()[0]:
        # Get friends and followers
        cur.execute("SELECT friends_list,followers_list FROM users WHERE user_id = {};".format(parent_id))
        ff = cur.fetchone()
        # if ff is None:
        #     return
        # if ff[0] is None:
        #     friends_n_followers =
        friends_n_followers = []
        if (ff is None) or ((ff[0] is None) and (ff[1] is None)):
            friends_n_followers = []
        elif (ff[0] is None) and (ff[1] is not None):
            friends_n_followers = ff[1]
        elif (ff[0] is not None) and (ff[1] is None):
            friends_n_followers = ff[0]
        elif (ff[0] is not None) and (ff[1] is not None):
            friends_n_followers = ff[0] + ff[1]
        else:
            print "\nsomething strange is happening in compute_candid_score!"
        if len(friends_n_followers) < 1:
            return
        # Calculate DF
        compute_df(cur=cur, conn=conn, tl_start_date=tl_start_date, tl_end_date=tl_end_date, user_set=friends_n_followers)
        # Find top TF-DF scores
        find_top_scores_to_expand(cur=cur, conn=conn, user_id_list=friends_n_followers, threshold_percentile=threshold_percentile, tf_type=tf_type)

# End of candid_tfidf.py
