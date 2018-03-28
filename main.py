import tweepy
import json
import csv
import time
import configparser
import os
import analyzer

## Reading config parameters
config = configparser.ConfigParser()
config.read('config.ini')

# Setting Twitter API credentials
consumer_key = config.get('api','consumer_key')
consumer_secret = config.get('api','consumer_secret')
access_key = config.get('api','access_key')
access_secret = config.get('api','access_secret')

# Setting Twitter handles
twitter_handles = config.get('api', 'twitter_handles').split()

# Converts to Tweepy Status object to JSON and add that as json attribute
@classmethod
def parse(cls, api, raw):
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status

tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse

def get_all_tweets(screen_name, file_name, writer):
    # authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # get new tweets
    new_tweets = api.user_timeline(screen_name=screen_name, count=100)

    # Writing tweets
    for tweet in new_tweets:
        t = json.loads(tweet.json)
        if "text" in t:
            t["text"] = t["text"].replace(",", "").replace("\n", " ").encode("utf-8", "ignore")
        writer.writerow(t)

if __name__ == '__main__':

    file_name = 'tweets-'+str(int(time.time()))+'.csv'

     # Writing header into file
    out_file = open(file_name, 'a')
    writer = csv.DictWriter(out_file, fieldnames=["id", "created_at", "text", "retweet_count", "favorite_count"], extrasaction='ignore',
                            delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    writer.writeheader()

    try:
        for twitter_handle in twitter_handles:
            print "Downloading Tweets of %s" % twitter_handle
            try:
                get_all_tweets(twitter_handle, file_name, writer)
            except Exception, e:
                print "Error in Downloading %s, %s" % (twitter_handle, e)
                continue
    except Exception as e:
        print "There is an exception: "+str(e)
    
    analyzer.analyze(os.path.abspath(file_name))
