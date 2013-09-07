from django.shortcuts import render_to_response
from django.http import HttpResponse , HttpResponseRedirect
from django.contrib.sites.models import Site

from emitta.models import Schools, Tweets
from twit.analyser import TwitterAnalyser

def get_tweets(request, access_key):
	if access_key == 'w398734jhje32kncy36ghsv':
		twitter, data, users, texts  = TwitterAnalyser(), twitter.get_data(), twitter.get_users(), twitter.get_text()
		update_db(users, texts)
		return HttpResponse('Ok')
	return HttpResponseRedirect('/')

def update_db(users, texts):
	schools = Schools.objects.all()
	tweets  = Tweets.objectsa.all()
	schools_names = [school.name for school in schools]
	all_users = [tweet.user for tweet in tweets]
	for i in range(len(texts)):
		school_name  = texts[i].encode('utf-8').split()[1]
		user =  users[i]

		if school_name in schools_names:
			school = schools.get(name = school_name)
			school.rank += 1
			school.save()

		else:
			school = Schools.objects.create(name = school_name, rank = 1)

		if user in all_users:
			user = tweets.get(user = user)
			user.no_tweets += 1
			user.save()

		else:
			user = Tweets.objects.create(text = texts[i].encode('utf-8'), user = user[i], no_tweets = 1)

			



