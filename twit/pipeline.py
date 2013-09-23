from analyser import TwitterAnalyser

import time 
import sqlite3
import os

path = os.getcwd()[:-4] + 'emit.db'
conn = sqlite3.connect(path)
cursor  = conn.cursor()

data = cursor.execute('select * from schools').fetchall()




# def update_db(users, texts):
# 	schools = cursor.execute('select * from schools').fetchall()
# 	school_names = [school[1] for school in schools]
# 	for i in range(len(texts)):
# 		school_name = texts[i].encode('utf-8').split()[1]
# 		if school_name in school_names:
# 			update_st = "update schools set rank=%d where name='%s'" % (update_rank(school_names, school_name), school_name)
# 			cursor.execute(update_st)
# 			conn.commit()

# 		else:
# 			insert_st = "insert into schools values ('%d','%s','%d')" %(school_names[-1][0] + 1, school_name, 1)
# 			cursor.execute(insert_st)
# 			conn.commit()


def update_rank(schools, name):
	for sch in schools:
		if sch[1] == name:
			return sch[2] + 1

def update_no_tweets(tweets, user):
	for tweet in tweets:
		if tweet[2] == user:
			return tweet[3] +  1

def pipeline():

	twitter = TwitterAnalyser()


	while time.localtime().tm_min < 60:
		data = twitter.get_data()
		users = twitter.get_users()
		texts = twitter.get_text()
		if twitter.is_same_key():
			time.sleep(twitter.next_conn_time)
			continue
		else:
			update_db(users, texts)
			print 'waiting for %s seconds' % twitter.next_conn_time
			time.sleep(twitter.next_conn_time)
			print 'going to twitter with id: %s' % twitter.max_key


def update_db(users, texts):
	schools = cursor.execute('select * from schools').fetchall()
	tweets = cursor.execute('select * from tweets').fetchall()

	
	all_users = [tweets[2] for tweet in tweets]
	school_names = [school[1].encode('utf-8') for school in schools]
	for i in range(len(texts)):
		try:
			school_name = texts[i].encode('utf-8').split()[1]
		except:
			school_name = texts[i].encode('utf-8')

		if school_name in school_names:
			print school_name
			update_st = "update schools set rank='%s' where name='%s'" % (update_rank(schools, school_name), school_name)
			
			try: 
				cursor.execute(update_st)
				conn.commit()
			except sqlite3.OperationalError:
				pass
		else:
			try:
				insert_st = "insert into schools ('name','rank') values ('%s','%s')" %(school_name, 1)
				cursor.execute(insert_st)
				conn.commit()
			except sqlite3.OperationalError:
				print 'OP ErrOR'

		user =  users[i]
		if user in all_users:
			update_st = "update tweets set no_tweets='%s' where user='%s'" % (update_no_tweets(tweets, user), user)
			cursor.execute(update_st)
			conn.commit()
		else:
			#text = texts[i].encode('utf-8')
			insert_st = """insert into tweets ('text','user', 'no_tweets') values ('%s','%s', '%s')""" %('some tweet', user, 1)
			cursor.execute(insert_st)
			conn.commit()






# def update_db(users, texts):
# 	schools = Schools.objects.all()
# 	schools_names = [school.name for school in schools]
# 	for i in range(len(users)):
# 		school_name = texts[i].encode('utf-8').split()[1]
# 		if school_name in schools_names:
# 			school = schools.get(name = school_name)
# 			school.rank += 1
# 			school.save()

# 		else:
# 			school = Schools.objects.create(name = school_name, rank = 1)



# def update_rank(schools, name):
# 	for sch in schools:
# 		if sch[1] == name: return sch[2] + 1

# def new_id(schools)

# def get_school(text):
# 	return text.split()[1]



	# for i in range(len(users)):
	# 	s  = users[i].encode('utf-8') +  '       ' + '         ' + text[i].encode('utf-8') + '\n'
	# 	db.write(s) 



pipeline()
