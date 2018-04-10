#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import json
import oauth2 as oauth
import time
import re
import pandas as pd
from collections import defaultdict
'''
Script that downloads tweets.

user_timeline() downloads 200 tweets for given user

my_timeline() downloads 200 tweets from clients timeline

searchApi() downloads 200 tweets for given query

'''

def user_timeline(user_name, min_id):
    timeline_endpoint = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name=" + user_name + \
                        "&count=200&since_id=" + str(
        min_id) + ""
    return result(timeline_endpoint)


def my_timeline():
    timeline_endpoint = "https://api.twitter.com/1.1/statuses/home_timeline.json?trim_user=false"
    return result(timeline_endpoint)


def searchApi(query, min_id):
    timeline_endpoint = "https://api.twitter.com/1.1/search/tweets.json?q=" + query + \
                        "&lang=en&result_type=recent&count=200&since_id=" + str(min_id) + "&lang=en"
    return result(timeline_endpoint)


def result(timeline_endpoint):
    global client
    response, data = client.request(timeline_endpoint)
    tweets = json.loads(data)
    return tweets


def exclude_link(str):
    i = str.find("http")
    return str[:i]


def exclude_emoji(s):
    l = list(s)
    df = pd.DataFrame({'phrases': [s]})
    for i in range(0, len(s)):
        for emoji in re.findall(u'[\U0001f300-\U0001f650]|[\u2000-\u3000]', s[i]):
            l[i] = " "
    # print emoji.encode('unicode-escape')
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    return ''.join(l)


# format pre datetime
def formTime(t):
    mon = t[4:7]
    day = t[8:10]
    tt = t[11:19]
    time_zone = t[20:25]
    year = t[26:30]
    stime = time.strptime(mon, "%b")
    mon = stime.tm_mon
    mon = "0" + str(mon)
    tt = year + "-" + mon[-2:] + "-" + day + " " + tt
    return tt


# tento search pozrie do databazy na najnovsie tweet_id a za neho bude hladat

def search_by_user(db, cur, user):
    cur.execute("SELECT user_name,last_tweet_id FROM search_by_user WHERE user_name LIKE '%" + user + "%'")
    # vyberie z databazy name, last_tweet_id, vrati ho ako slovnik kde klucom je meno, hodnotou je id
    a = dict(cur.fetchall())
    min_id = 1
    if a == {}:
        cur.execute("INSERT INTO search_by_user(user_name, user_id, last_tweet_id) VALUES(%s,%s,%s)", (user, "0", "0"))
    else:
        min_id = a[user]

    max_id = 0
    query_id = -1
    tweets = user_timeline(user, min_id)
    tweet_list = []
    for tweet in tweets:
        tweet_list.append(tweet)

    if not tweets == []:
        # stiahne tweety a ulozi ich do tab (databazy)
        for tweet in reversed(tweet_list):
            name = tweet['user']['name']
            i = tweet['user']['id']
            t = tweet['created_at']
            retweet_count = tweet['retweet_count']
            favorite_count = tweet['favorite_count']
            text = exclude_link(tweet['text'])
            name = exclude_emoji(name)
            text = exclude_emoji(text)
            print text
            tweet_id = tweet['id']
            t = formTime(t)
            max_id = max(tweet_id, max_id)
            min_id = min(tweet_id, min_id)
            cur.execute(
                "INSERT INTO tab (retweet_count, favorite_count,user_name, user_id,text, date,tweet_id,company_id) \
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                (retweet_count, favorite_count, name, i, text, t, tweet_id, query_id))
            db.commit()
        # ulozi najnovsie last_tweet_id a user_id, 2. uklada len preto, ze ak pred tymto searchom sme
        # takehoto usera nemali, tak nemame ani jeho idcko- cize ho potrebujeme ulozit
        cur.execute("UPDATE search_by_user SET user_id = '" + str(i) + "', last_tweet_id = '" + str(
            max_id) + "' WHERE user_name = '" + user + "'")
        db.commit()
    else:
        cur.execute(
            "DELETE FROM search_by_user WHERE user_name = '" + user + "' and user_id = 0  and last_tweet_id = 0 limit 1")


def search_by_query(db, cur, query):
    cur.execute("SELECT * FROM search_by_query WHERE search_query LIKE '%" + query + "%'")
    a = cur.fetchall()
    min_id = 1
    max_id = 0

    if a == ():
        cur.execute(
            "SELECT `AUTO_INCREMENT` FROM  INFORMATION_SCHEMA.TABLES \
            WHERE TABLE_SCHEMA = 'tweetdata' and table_name \= 'search_by_query';")
        db.commit()
        temp = cur.fetchall()
        query_id = temp[0][0]
        cur.execute("INSERT INTO search_by_query(search_query, last_tweet_id) VALUES(%s,%s)", (query, "0"))
        db.commit()
    else:
        print  a
        min_id = a[2]
        query_id = a[0]

    tweets = searchApi(query, min_id)
    tweet_list = []
    for tweet in tweets['statuses']:
        tweet_list.append(tweet)

    if not tweet_list == []:
        for tweet in reversed(tweet_list):
            name = tweet['user']['name']
            i = tweet['user']['id']
            t = tweet['created_at']
            retweet_count = tweet['retweet_count']
            favorite_count = tweet['favorite_count']
            text = exclude_link(tweet['text'])
            name = exclude_emoji(name)
            print text, name
            text = exclude_emoji(text)
            tweet_id = tweet['id']
            t = formTime(t)
            max_id = max(tweet_id, max_id)
            min_id = min(tweet_id, min_id)
            command = u"INSERT INTO tab (retweet_count, favorite_count,user_name, user_id,text, date, tweet_id,company_id)\
             VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(command, (retweet_count, favorite_count, name, i, text, t, tweet_id, query_id))
            db.commit()
        # ulozi najnovsie last_tweet_id a user_id, 2. uklada len preto, ze ak pred tymto searchom sme takehoto usera
        # nemali, tak nemame ani jeho idcko- cize ho potrebujeme ulozit
        cur.execute(
            "UPDATE search_by_query SET last_tweet_id = '" + str(max_id) + "' WHERE search_query = '" + query + "'")
        db.commit()
    else:
        cur.execute("DELETE FROM search_by_query WHERE search_query = '" + query + "' and last_tweet_id = 0 limit 1")


# access to twitter API
ckey = 'aw5e8E7vYZubrrn4Z3FxPgvwL'
csecret = 'sLPu7sykydvGnX1flxN27p26AMH5VhOALR6tqzAJ1kMfSPAntO'
atoken = '882898101740163076-8CSSz9TmOb0IpFQkoSrVlknUhRuGv8G'
asecret = '367tQAoU08JpWP3RFSxgyI6s6DDWV3C9msqRmbtQmKquI'
consumer = oauth.Consumer(key=ckey, secret=csecret)
access_token = oauth.Token(key=atoken, secret=asecret)
client = oauth.Client(consumer, access_token)

# tweets=user_timeline("Reuters", 1)


# access to database in MySql
db = MySQLdb.connect(host="localhost", user="andrej", passwd="password", db="tweetdata", use_unicode=True,
                     charset='utf8mb4')
cur = db.cursor()

search_by_user(db, cur, "@NASA")

db.close()
