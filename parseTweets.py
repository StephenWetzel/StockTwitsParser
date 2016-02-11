import MySQLdb as mdb
import urllib2
import json

symbols = ["AAPL", "SPY", "IBM"]
#symbol = "CME/CLF2016"
baseUrl = "https://api.stocktwits.com/api/2/streams/symbol/"
urlSuffix = ".json"

con = mdb.connect('localhost', 'dbuser', 'dbuser', 'mydb');
with con: #this creates the two tables, symbols and prices, and drops them if they already exist
	cur = con.cursor()
	sql = "SELECT * FROM Symbols WHERE type='index'"
	cur.execute(sql)
	symbols = cur.fetchall()


for row in symbols:
	exchange = row[0]
	symbol = row[1]
	futureType = row[2]
	futureCat  = row[3]
	expireMonth = row[4]
	expireYear = row[5]
	stillUpdated = row[6]
	url = baseUrl + symbol + urlSuffix
	#https://api.stocktwits.com/api/2/streams/symbol/AAPL.json
	
	data = urllib2.urlopen(url).read()
	data = json.loads(data)
	
	for tweet in data['messages']:
		messageId = tweet['id']
		body = tweet['body'].encode('utf-8')
		timestamp = tweet['created_at'].encode('utf-8')
		username = tweet['user']['username'].encode('utf-8')
		
		
		print("Message ID: " + str(messageId))
		print("Message body: " + str(body))
		print("Message Time: " + str(timestamp))
		print("Message Username: " + str(username))
		print "\n"
		
		sql = "INSERT INTO Tweets(timestamp, message_id, body, username) VALUES ('%s', '%s', '%s', '%s') ON DUPLICATE KEY UPDATE timestamp=VALUES(timestamp), message_id=VALUES(message_id), body=VALUES(body), username=VALUES(username)" % \
		(timestamp, messageId, body, username)
		#print sql
		cur.execute(sql)
	con.commit()
		
		
