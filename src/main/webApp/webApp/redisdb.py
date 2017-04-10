import redis
import pickle


class ConctDB(object):
    def __init__(self):
        self.conn = redis.StrictRedis(host='localhost', port=6379, db=0)

    def set(self, key, value):
        value = pickle.dumps(value)
        self.conn.set(key, value)

    def get(self, key):
        value = self.conn.get(key)
        if value != None:
            value = pickle.loads(value)
            return value
        else:
            return []