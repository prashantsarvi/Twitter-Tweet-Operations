from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy

import json
from pymongo import MongoClient

storeJSON = []


class TweetStream(StreamListener):

    def __init__(self):

        super().__init__()
        self.countTweets = 0

    def on_data(self, dataReceived):
        global storeJSON
        if 0 <= self.countTweets <= 2500:
            self.countTweets += 1
            dataReceived = json.loads(dataReceived)

            storeJSON.append(dataReceived)
            return True
        else:
            print(storeJSON)
            print("Limit Reached")
            return False

    def on_error(self, errorCheck):
        print(errorCheck)
        return False


if __name__ == "__main__":
    myclient = MongoClient("mongodb+srv://TwitterDB:twitterextraction@twittercluster.udioj.mongodb.net/test")
    db = myclient["RawDB"]
    Collection = db["TestTweet"]

    tweetListener = TweetStream()
    authentication = OAuthHandler("rfQqCDztquIOPK0yKDB30SBEa", "fPHxHfJR9QMeVaK5pm9k1poyLWWJzbARXmjkwwm5kwgwdDfJPE")
    authentication.set_access_token("1322905249078652928-ijzNu28JIYUkQsc3sDzN5bAJnDqfRa",
                                    "5zo03NCBjUYGEMuQPmle0juHQb1FEEJs0Sv7uEJdQFfEf")
    myApi = tweepy.API(authentication)
    streamingTweets = Stream(authentication, tweetListener)
    streamingTweets.filter(track=['Storm', 'Winter', 'Canada', 'Temperature', 'Flu', 'Snow', 'Indoor', 'Safety'])
    myApi.search('Storm or Winter or Canada or Temperature or Flu or Snow or Indoor or Safety')
    Collection.insert_many(storeJSON)
