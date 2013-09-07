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
			# print 'thgfjghierotfpoeporpoi', sch[2] + 1 
			return sch[2] + 1

def pipeline():

	twitter = TwitterAnalyser()


	while time.localtime().tm_min < 60:
		data = twitter.get_data()
		users = twitter.get_users()
		texts = twitter.get_text()

		update_db(users, texts)
		print 'waiting for %s seconds' % twitter.next_conn_time
		time.sleep(twitter.next_conn_time)
		print 'go to twitter with id: %s' % twitter.max_key


def update_db(users, texts):
	schools = cursor.execute('select * from schools').fetchall()
	print schools
	school_names = [school[1] for school in schools]
	print school_names
	for i in range(len(texts)):
		try:
			school_name = texts[i].encode('utf-8').split()[1]
		except:
			school_name = texts[i].encode('utf-8')
		print 'school   ' + school_name + '   the'
		if school_name in school_names:
			print school_name
			update_st = "update schools set rank='%d' where name='%s'" % (update_rank(schools, school_name), school_name)
			cursor.execute(update_st)
			conn.commit()

		else:
			try:
				insert_st = "insert into schools ('name','rank') values ('%s','%d')" %(school_name, 1)
				cursor.execute(insert_st)
				conn.commit()
			except sqlite3.OperationalError:
				print 'OP ErrOR'



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
