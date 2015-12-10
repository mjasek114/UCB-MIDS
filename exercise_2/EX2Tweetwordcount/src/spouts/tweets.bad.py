from __future__ import absolute_import, print_function, unicode_literals

import itertools, time
import tweepy, copy 
import Queue, threading
import simplejson

from streamparse.spout import Spout

################################################################################
# Twitter credentials
################################################################################
twitter_credentials = {
    "consumer_key"        :  "YAMNPe4Ao1jlHr5bq08aYtOR6",
    "consumer_secret"     :  "XcXA5qjWbeaEE5puVYZN7PfRj9dRRByYLnUBuzfkjlGlWDwQL7",
    "access_token"        :  "97372015-0nwwZaSoyv90bOoNG0MlpmKNEGag3C7LcuDfTKIYg",
    "access_token_secret" :  "5favVcQfRujhC8pP5cdtqGmO89BySKLRmJG7kjbRBwVy2",
}

def auth_get(auth_key):
    if auth_key in twitter_credentials:
        return twitter_credentials[auth_key]
    return None

################################################################################
# Class to listen and act on the incoming tweets
################################################################################
class TweetStreamListener(tweepy.StreamListener):

    def __init__(self, listener):
        self.listener = listener
        super(self.__class__, self).__init__(listener.tweepy_api())

#    def on_status(self, status):
#        print(status.text)
#        self.listener.queue().put(status.text, timeout = 0.01)
#        return True

    def on_data(self, data):
        try:
            self.dataJson =simplejson.loads(data[:-1])
            self.dataJsonText = self.dataJson["text"].lower()
            self.listener.queue().put(self.dataJsonText, timeout = 0.01)
            print(self.dataJsonText)
            return True
        except Exception, e:
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            pass
  
    def on_error(self, status_code):
        return True # keep stream alive
  
    def on_limit(self, track):
        return True # keep stream alive

class Tweets(Spout):

    def initialize(self, stormconf, context):
        self._queue = Queue.Queue(maxsize = 100)

        consumer_key = auth_get("consumer_key") 
        consumer_secret = auth_get("consumer_secret") 
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

        if auth_get("access_token") and auth_get("access_token_secret"):
            access_token = auth_get("access_token")
            access_token_secret = auth_get("access_token_secret")
            auth.set_access_token(access_token, access_token_secret)

        self._tweepy_api = tweepy.API(auth)

        # Create the listener for twitter stream
        listener = TweetStreamListener(self)

        # Create the stream and listen for english tweets
        stream = tweepy.Stream(auth, listener, timeout=None)
#        stream.filter(track=["a", "A", "the", "THE", "i", "I", "you", "YOU", "u", "U"], async=True)
        stream.sample()

    def queue(self):
        return self._queue

    def tweepy_api(self):
        return self._tweepy_api

    def next_tuple(self):
        try:
            tweet = self.queue().get(timeout = 0.1)
            print(tweet)
            if tweet:
                self.queue().task_done()
                self.emit([tweet])
 
        except Queue.Empty:
            self.log("Empty queue exception ")
            time.sleep(0.1) 

    def ack(self, tup_id):
        pass  # if a tuple is processed properly, do nothing

    def fail(self, tup_id):
        pass  # if a tuple fails to process, do nothing
