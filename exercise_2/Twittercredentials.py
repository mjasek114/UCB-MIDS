import tweepy

consumer_key = "YAMNPe4Ao1jlHr5bq08aYtOR6";
#eg: consumer_key = "YisfFjiodKtojtUvW4MSEcPm";


consumer_secret = "XcXA5qjWbeaEE5puVYZN7PfRj9dRRByYLnUBuzfkjlGlWDwQL7";
#eg: consumer_secret = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";

access_token = "97372015-0nwwZaSoyv90bOoNG0MlpmKNEGag3C7LcuDfTKIYg";
#eg: access_token = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";

access_token_secret = "5favVcQfRujhC8pP5cdtqGmO89BySKLRmJG7kjbRBwVy2";
#eg: access_token_secret = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)



