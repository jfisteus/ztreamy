# streamsem: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2012 Norberto Fernandez Garcia
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
import tweepy


class TwitterRESTclient():
    def __init__(self, user):
        self.access_token = \
                          "414308911-bMj5UFGvu1FNVpiyi4g3AvPbN5zASIcxIX8p4wGY"
        self.access_token_secret = "jpfuE1DqAUxDSyaqbDwaRufbQSUMwwNkP6ygHsFw2I"
        self.consumer_key = "oTOPoCBWwh4b0YhCGqbbg"
        self.consumer_key_secret = \
                                 "yQcbt1eUm8YO4JQk8tvqzUfZY7aP0s0B1BXOxH0hbqA"
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
                # No quiero descargarlos todos... solo los ultimos
                # (Desde un ID dado, 200 como maximo)
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
                print "Twitter Error: %s" % str(e)
                pass
        return tweets_list

def main():
    client = TwitterRESTclient("cnn")
    tweets = client.get_tweets()
    for tweet in tweets:
        print tweet.id,"\t",tweet.created_at

if __name__ == "__main__":
    main()


