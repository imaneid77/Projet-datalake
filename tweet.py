import tweepy

# Remplacez par votre Bearer Token
bearer_token = "AAAAAAAAAAAAAAAAAAAAABadyQEAAAAAKcfzn48HMTfp4Nehnvw5uxeO37c%3Dfl3TDKjpvl8f8H7186Ywycf0Lqyuiz2CvgnKnilxYCpgIUcTsg" 


# =====================================
# access token : 1882069259511009280-RheITcV9xt8Ni2AHzdT4NMRDIDO17S 
# access token secret : QxSHayGIEGAPhKjHPkQRxltLroBphVgeUlfWD3REyNNs7
# api key : fUFhnBC88iYhdQcg5bEyHDGdA 
# api key secret : bXiCulIYqwoUTjdO63IhdpjYPpYzX6yBmvXlk5jVRd9AmCh9Xs
# bearer : AAAAAAAAAAAAAAAAAAAAABadyQEAAAAAKcfzn48HMTfp4Nehnvw5uxeO37c%3Dfl3TDKjpvl8f8H7186Ywycf0Lqyuiz2CvgnKnilxYCpgIUcTsg
# =====================================


# Création du client Tweepy pour l’API v2
client = tweepy.Client(bearer_token=bearer_token)

# Exemple : recherche de tweets contenant le mot "python"
query = "python -is:retweet lang:fr"
tweets = client.search_recent_tweets(query=query, max_results=10)

# Parcours des résultats
for tweet in tweets.data:
    print(tweet.text)
