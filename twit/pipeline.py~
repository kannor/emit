from twit.analyser import TwitterAnalyser


import time
import sqlite3

db = open('db', 'a')

def pipeline():
	twitter =  TwitterAnalyser()

	while time.localtime().tm_min < 60:
		data = twitter.get_data()
		users = twitter.get_users()
		text = twitter.get_text()

		update_db(users, text)
		print 'waiting for %s seconds' % twitter.next_conn_time
		time.sleep(twitter.next_conn_time)

		print 'go to twitter with id: %s' % twitter.max_key


def update_db(users, text):
	for i in range(len(users)):
		s  = users[i].encode('utf-8') +  '       ' + '         ' + text[i].encode('utf-8') + '\n'
		db.write(s) 

pipeline()
