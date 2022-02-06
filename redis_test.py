import time

import redis
import warnings
warnings.filterwarnings('ignore')

r = redis.Redis(
    host='localhost',
    port=6379
)

r.flushdb()

r.hmset("tweet:1", {"tweet_id": 1, "user_id": 2, "date": time.time(), "text": "testtext"})
r.lpush("tweets", "tweet:1")
r.hset("tweet:2", "2", "two t tw")



print(str(r.hgetall('tweet:1')[b'text'])[2:-1])
key = str(r.lrange("tweets", 0, -1)[0])[2:-1]
print(key)
print(str(r.hgetall(key)[b'text'])[2:-1])
