from __future__ import absolute_import, print_function, unicode_literals

import itertools, time
import tweepy, copy 
import Queue, threading

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

    def on_status(self, status):
        self.listener.queue().put(status.text, timeout = 0.01)
        return True
  
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
        stream.filter(languages=["en"], track=["a", "the", "i", "you", "u"], async=True)

        self.sentences = [ "She advised him to take a long holiday, so he immediately quit work and took a trip around the world", "I was very glad to get a present from her", "he will be here in half an hour", "She saw him eating a sandwich", ]
        self.sentences = itertools.cycle(self.sentences)
#        self.count = 0

    def queue(self):
        return self._queue

    def tweepy_api(self):
        return self._tweepy_api

    def next_tuple2(self):
        try:
            tweet = self.queue().get(timeout = 0.1) 
            if tweet:
                self.queue().task_done()
                self.emit([tweet])
 
        except Queue.Empty:
            self.log("Empty queue exception ")
            time.sleep(0.1) 

    def next_tuple(self):
 #       if self.count < 144:
            tweet = next(self.sentences)
            self.emit([tweet])
 #           self.count += 1

    def ack(self, tup_id):
        pass  # if a tuple is processed properly, do nothing

    def fail(self, tup_id):
        pass  # if a tuple fails to process, do nothing
