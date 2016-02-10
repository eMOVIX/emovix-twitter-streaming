__author__ = 'Jordi Vilaplana'

import tweepy
import pymongo
from pymongo import MongoClient
import json
import logging

logging.basicConfig(
    filename='emovix_twitter_streaming.log',
    level=logging.WARNING,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%d-%m-%y %H:%M')

# Configuration parameters
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""
database_name = ""
source_box = ""

#ignored_languages = ["ja", "in", "tr", "tl", "ar", "ru", "th"]

ignored_tweet_fields = ["contributors", "truncated", "is_quote_status", "in_reply_to_status_id", "in_reply_to_screen_name", "geo",
                        "in_reply_to_user_id", "favorited", "in_reply_to_user_id_str", "filter_level", "in_reply_to_status_id_str"]

ignored_user_fields = ["follow_request_sent", "profile_use_background_image", "default_profile_image", "verified", "profile_image_url_https",
                       "profile_sidebar_fill_color", "profile_text_color", "profile_sidebar_border_color", "id_str", "profile_background_color",
                       "profile_background_image_url_https", "utc_offset", "profile_link_color", "profile_image_url", "following",
                       "profile_background_image_url", "profile_background_tile", "notifications", "created_at", "contributors_enabled",
                       "protected", "default_profile", "is_translator"]

client = None
db = None

class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        self.db = pymongo.MongoClient().emovix

    def on_data(self, data):
        tweet = json.loads(data)

        # This code ignores limit notices
        # https://dev.twitter.com/streaming/overview/messages-types#limit_notices
        if tweet.get('limit'):
            logging.debug('Limit notice received: ' + str(tweet['limit']['track']))
            self.db.twitterLimitNotice.insert(tweet)
            return True

        #if tweet.get('lang') in ignored_languages:
        #    return True

        user = tweet['user']

        for field in ignored_tweet_fields:
            del tweet[field]

        for field in ignored_user_fields:
            del tweet['user'][field]

        # We mark each tweet with its source bounding box (defined in the config.json file)
        tweet['source_box'] = source_box
        self.db.twitterStatus.update(tweet, tweet, upsert=True)
        self.db.twitterUser.update({"screen_name": tweet['user']['screen_name']}, user, upsert=True)
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
        source_box = config['source_box']

    client = MongoClient('mongodb://localhost:27017/')
    db = client[database_name]

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    while True:
        try:
            logging.debug('Connecting to Twitter stream ...')
            stream = tweepy.streaming.Stream(auth, CustomStreamListener(api))
            stream.filter( locations = [-180, -90, 180, 90] )
        except Exception as e:
            # Oh well, reconnect and keep trucking
            logging.error(e.__class__)
            logging.error(e)
            continue
        except KeyboardInterrupt:
            stream.disconnect()
            break
