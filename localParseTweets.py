import MySQLdb as mdb
import urllib2
import json
import sys


tweetsFileName = "stock.json"

#get all the symbols stored in the db:
con = mdb.connect('localhost', 'dbuser', 'dbuser', 'homestead');
with con:
	cur = con.cursor()
	sql = "SELECT * FROM Symbols WHERE type='index'"
	cur.execute(sql)
	symbolRows = cur.fetchall()

#convert db rows to a list of symbols that we care about
symbols = []
for row in symbolRows:
	symbol = row[2]
	symbols.append(symbol)

try:
	with open(tweetsFileName, 'r') as iFile:
		for line in iFile:

			#testing to see if this line has the symbol we are looking for is a huge bottle neck
			#we need to try different approaches to see which is fastest
			#1. parse line as json and access the tag
				#if we do this then we need a loop over the symbols, there usually is only 1 or a few, but the loop overhead could be an issue
			#2. regex match on line
			#3. python substring match on line
				#with these two, we could either search the full line, or the json parsed message body

			#some test results, 100 MB input:
			#parse json, use symbol in body: 2.463s
			#parse json for each line, don't do anything else, just checking the overhead of the json parse: 2.458s
			#just do if symbol in line: 0.302s

			#Time to run on full 2014 data, search for AAPL, add tweets to DB: 1m1.396s

			if any(symbol in line for symbol in symbols):
				tweet = json.loads(line)
				
				body = tweet['body'].encode('utf-8')
				messageId = tweet['id']
				timestamp = tweet['created_at'].encode('utf-8')
				username = tweet['user']['username'].encode('utf-8')
				
				# print("Message ID: " + str(messageId))
				# print("Message body: " + str(body))
				# print("Message Time: " + str(timestamp))
				# print("Message Username: " + str(username))
				# print "\n"
				
				sql = "INSERT INTO Tweets(timestamp, message_id, body, username) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE timestamp=VALUES(timestamp), message_id=VALUES(message_id), body=VALUES(body), username=VALUES(username)"
				#print sql
				cur.execute(sql, (timestamp, messageId, body, username))
		con.commit()

except (IOError, ValueError): #file does not exist
	print "File Not Found"

