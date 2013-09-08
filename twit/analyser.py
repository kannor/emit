from connector import connect

class TwitterAnalyser:

	def __init__(self):
		self.max_key = 0
		self.old_max_key = ''
		self.next_conn_time = 10
		self.client = connect()
		self.data = {}

	def get_data(self):
		self.data = self.client.search.tweets(q = 'a', max_key = self.max_key)
		self.max_key = self.data.get('search_metadata', None).get('max_id', None)
		return self.data

	def get_users(self):
		return [i.get('screen_name') for i in [i.get('user') for i in self.data.get('statuses')]]

	def get_text(self):
		return [i.get('text') for i in self.data.get('statuses')]

	def is_valid(self, tweet):
		return 'RT' not in tweet

	def is_same_key(self):
		return self.max_key is self.old_max_key

	def update_next_conn_time(self):
		if not len(self.data.get('statuses')):
			self.next_conn_time += 20
			return
		self.next_conn_time = 10

	def pause_pipeline(self):
		if self.is_same_key():
			self.next_conn_time *= 100
			return
