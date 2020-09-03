from __future__ import print_function
import json
from kafka import KafkaProducer, KafkaClient
import tweepy
access_token = "1276871198543093763-gBx3jtk56OWNFk9cjTbKnKmcUBkDgj"
access_token_secret =  "7hckEvlAdZTeAEDkkn9guQJ4DvFdjOLXy50XHbrq2HJSL"
consumer_key =  "NRqLKTJB9LFcO1ztoKOafwRMk"
consumer_secret =  "6H3U3y7gG1yropk4sHej7g0Y9j21BKoAaiPy2XLHKYkLX1Qj05"
# Words to track
WORDS = ['shoes offers', 'boots offers','sneakers offers','girls shoes offers','#shoesoffers']
class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.
    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print("Error received in kafka producer " + repr(status_code))
        return True # Don't kill the stream
    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        try:
            producer.send('offers', data.encode('utf-8'))
        except Exception as e:
            print(e)
            return False
        return True # Don't kill the stream
    def on_timeout(self):
        return True # Don't kill the stream