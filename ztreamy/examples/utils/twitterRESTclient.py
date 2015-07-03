# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2015 Norberto Fernandez Garcia
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
#

""" A client for Twitter REST API based on tweepy that accesses and downloads a user timeline.
    NOTE: This client requires access/consumer tokens as provided by Twitter when registering
    an application to be available on a file named twitter_config.py with the same format as
    a Java properties file: propName = propValue
"""

from __future__ import print_function

import tweepy
import argparse
import twitter_config

class TwitterRESTclient():    
    """ A client for Twitter REST API based on tweepy that accesses and downloads a user timeline
    """
    
    def __init__(self, user):
        self.access_token = twitter_config.access_token
        self.access_token_secret = twitter_config.access_token_secret
        self.consumer_key = twitter_config.consumer_key
        self.consumer_key_secret = twitter_config.consumer_key_secret
        self.api = self.get_access()
        self.user = user

    def get_access(self):
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_key_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        return tweepy.API(auth)

    def get_tweets(self, last_id=0):
        rate_limit = self.api.rate_limit_status()
        remaining_hits = rate_limit['remaining_hits']
        tweets_list = []

        if remaining_hits > 0:
            try:
                # Do not download everything, just last 200 tweets since a concrete tweet id
                if last_id == 0:
                    public_tweets = \
                       self.api.user_timeline(screen_name=self.user, count=200)
                else:
                    public_tweets = \
                       self.api.user_timeline(screen_name=self.user,
                                              count=200, since_id=last_id)
                public_tweets.reverse()
                for tweet in public_tweets:
                    tweets_list.append(tweet)
            except tweepy.error.TweepError as e:
                print("Twitter Error: {}".format(e))
                pass
        return tweets_list

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", dest="user", required=True, 
                  help="the identifier of a registered GeoNames user (e.g. demo)")    
    options = parser.parse_args()
    
    client = TwitterRESTclient(options.user)
    tweets = client.get_tweets()
    for tweet in tweets:
        print('{id}\t{ts}'.format(id=tweet.id, ts=tweet.created_at))

if __name__ == "__main__":
    main()


