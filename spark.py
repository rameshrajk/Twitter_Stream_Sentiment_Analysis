from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from geopy.geocoders import Nominatim
from textblob import TextBlob
import nltk
#nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch



TCP_IP = 'localhost'
TCP_PORT = 9001



#geolocator = Nominatim(user_agent="TwitterApp")




def processTweet(tweet):

    tweetData = tweet.split("::")

    if len(tweetData) > 1:

        location = tweetData[0]
        lat = tweetData[1]
        lon = tweetData[2]
        state = tweetData[3]
        country = tweetData[4]
        text = tweetData[5]

        #location = geolocator.geocode(tweetData[0], addressdetails=True)
        #lat = location.raw['lat']
        #lon = location.raw['lon']
        #state = location.raw['address']['state']
        #country = location.raw['address']['country']

    # Sentiment analysis
        #sentiment = TextBlob(text).sentiment
        #sentiment.split("::")
        sid = SentimentIntensityAnalyzer()
        #sentiment = sid.polarity_scores(text)
        if float(sid.polarity_scores(text)["compound"]) > 0:
            sentiment = "Positive"
        elif float(sid.polarity_scores(text)["compound"]) == 0:
            sentiment = "Neutral"
        else:
            sentiment = "Negative"

	
        #print("\n\n=========================\ntweet: ", tweet)
        #print("Raw location from tweet status: ", location)
        #print("lat: ", lat)
        #print("lon: ", lon)
        #print("state: ", state)
        #print("country: ", country)
        #print("Text: ", text)
        #print("Sentiment: ", sentiment)



    # Post the index on ElasticSearch
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        
        if sentiment!=None and lat!=None and lon!=None and state!=None and country!=None:
            mappings = {
                    "mappings": {
                        "properties": {
                            "location": {
                                "type": "geo_point"
                            },
                            "state": {
                                "type": "keyword"
                            },
                            "country": {
                                "type": "keyword"
                            },
                            "sentiment": {
                                "type": "keyword"
                            }
                        }
                    }
            }

            es.indices.create(index='twitterk', body=mappings, ignore=400)
            es_entries = {"location": {"lat": lat,"lon": lon}, "state": state, "country": country, "sentiment": sentiment}
            es.index(index="twitterk", body=es_entries)

            # creating dictionary for indexing on ElasticSearch
            #esDocument = {"lat": lat, "lon": lon, "state": state, "country": country, "sentiment": sentiment}

            #indexing on ElasticSearch
            #es.index(index='tweet-sentiment', doc_type='default', body=esDocument)


# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))


ssc.start()
ssc.awaitTermination()
