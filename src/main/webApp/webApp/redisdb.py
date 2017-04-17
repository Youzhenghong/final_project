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
    def initMessageQueue(self, queue_name):
        self.conn.delete(queue_name)


    def push(self, queue_name, item):
        item = pickle.dumps(item)
        self.conn.lpush(queue_name, item)

    def pop(self, queue_name):
        item = self.conn.rpop(queue_name)
        item = pickle.loads(item)
        return item

    def isEmpty(self, queue_name):
        if self.conn.llen(queue_name):
            return False
        else:
            return True

    def length(self, queue_name):
        return self.conn.llen(queue_name)

    def pubsub(self):
        return self.conn.pubsub()

    def publish(self, channel, data):
        data = pickle.dumps(data)
        self.conn.publish(channel, data)