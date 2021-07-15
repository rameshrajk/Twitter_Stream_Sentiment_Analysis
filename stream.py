import tweepy
import socket
import re
from gmaps import Geocoding
from geopy.geocoders import Nominatim

# import preprocessor



# insert Twitter keys here
ACCESS_TOKEN = '-'
ACCESS_SECRET = '-'
CONSUMER_KEY = '-' 
CONSUMER_SECRET = '-'


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

api = Geocoding(api_key='AIzaSyC0qf_NVGkKiBZ60szdVg-wsUqU4mFMS-E')
geolocator = Nominatim(user_agent="tweet")

hashtag = '#COVID19'

TCP_IP = 'localhost'
TCP_PORT = 9001




def preprocessing(tweet):
    
    # preprocess the tweets  
    # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc
    tweet = re.sub(r'[^\w.\s#@/:%,_-]', "", tweet)
    return tweet




def getTweet(self, status):

    tweet = ""
    location = ""
    lat = ""
    lon = ""
    state = ""
    country = ""

    #location stuff
    try:
        location = geolocator.geocode(status.user.location, addressdetails=True)
        lat = str(location.raw['lat'])
        lon = str(location.raw['lon'])
        state = str(location.raw['address']['state'])
        country = str(location.raw['address']['country'])
    except:
        lat = lon = state = country = None


    #if location:
    #    geo = api.geocode(location)
    #    if geo:
    #        coordinate = geo[0]['geometry']['location']
    #        location = str(coordinate['lng']) + "," + str(coordinate['lat'])
    #    else:
    #        location = ''
    #else:
    #    location = ''

    # check if retweet
    if hasattr(status, "retweeted_status"):
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    return str(location), lat, lon, state, country, preprocessing(tweet)





# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, lat, lon, state, country, tweet = getTweet(self, status)

        if (location != None and tweet != None and lat != None and lon != None and state != None and country != None):
            tweetLocation = location + "::" + lat + "::" + lon + "::" + state + "::" + country + "::" + tweet + "\n"
            print(status.text)
            conn.send(tweetLocation.encode('utf-8'))

        return True


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print("Error " + status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])


