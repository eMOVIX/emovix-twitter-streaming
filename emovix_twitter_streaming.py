__author__ = 'Jordi Vilaplana'

import httplib
import tweepy
import pymongo
from pymongo import MongoClient
import json
import logging

logging.basicConfig(filename='emovix_twitter_streaming.log',level=logging.WARNING)

# Configuration parameters
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""
database_name = ""

client = None
db = None

class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        self.db = pymongo.MongoClient().emovixtest

    def on_data(self, data):
        tweet = json.loads(data)

        # This code ignores limit notices
        # https://dev.twitter.com/streaming/overview/messages-types#limit_notices
        if tweet.get('limit'):
            logging.debug('Limit notice received: ' + str(tweet['limit']['track']))
            return True

        twitterStatus = db.twitterStatus
        self.db.twitterStatus.update(tweet, tweet, upsert=True)
        return True

    def on_error(self, status):
        logging.error('CustomStreamListener on_error')
        logging.error(status)
        return True

    def on_timeout(self):
        logging.error('CustomStreamListener on_timeout')
        return True # Don't kill the stream

if __name__ == '__main__':
    logging.debug('emovix_twitter_streaming.py starting ...')

    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)
        access_token = config['access_token']
        access_token_secret = config['access_token_secret']
        consumer_key = config['consumer_key']
        consumer_secret = config['consumer_secret']
        database_name = config['database_name']

    client = MongoClient('mongodb://localhost:27017/')
    db = client[database_name]

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    while True:
        try:
            logging.warning('Connectiong ...')
            stream = tweepy.streaming.Stream(auth, CustomStreamListener(api))
            stream.filter( locations = [-180, -90, 180, 90] )
        except Exception as e:
            # Oh well, reconnect and keep trucking
            logging.error(e.__class__)
            continue
        except KeyboardInterrupt:
            stream.disconnect()
            break
