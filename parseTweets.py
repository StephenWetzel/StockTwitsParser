import MySQLdb as mdb
import urllib2
import json

symbols = ["AAPL", "SPY", "IBM"]
#symbol = "CME/CLF2016"
baseUrl = "https://api.stocktwits.com/api/2/streams/symbol/"
urlSuffix = ".json"

for symbol in symbols:
	url = baseUrl + symbol + urlSuffix
	#https://api.stocktwits.com/api/2/streams/symbol/AAPL.json
	
	data = urllib2.urlopen(url).read()
	data = json.loads(data)
	
	for tweet in data['messages']:
		print("Message ID: " + str(tweet['id']))
		print("Message body: " + str(tweet['body']))
		print("Message Time: " + str(tweet['created_at']))
		print("Message Username: " + str(tweet['user']['username']))
		print "\n"
