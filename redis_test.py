import random
import time

import redis
import warnings

from RedisInteract import RedisInteract
from Tweet import Tweet

warnings.filterwarnings('ignore')

# r = redis.Redis(
#     host='localhost',
#     port=6379,
#     decode_responses=True
# )
#
# r.flushall()

# r.hmset("tweet:1", {"tweet_id": 1, "user_id": 2, "date": time.time(), "text": "testtext"})
# r.lpush("tweets", "tweet:1")
# r.hset("tweet:2", "2", "two t tw")
#
# print(r.hgetall('tweet:1')[b'text'].decode("utf-8"))
# key = r.lrange("tweets", 0, -1)[0].decode("utf-8")
# print(key)
# print(r.hgetall(key)[b'text'].decode("utf-8"))
#
# print(r.get("user1"))
# r.rpush("user1", "user2")
# r.rpush("user1", "user3")
# print(r.lrange("user1", 0, -1))
#
# print("user_10"["user_10".index("_") + 1:])
#
# print('starting class test')
# red_interact = RedisInteract()
#
# red_interact.insert_follow(2, 3)
# red_interact.insert_follow(3, 2)
# tweet1 = Tweet(3, 1, time.time(), "sampletext1")
# tweet2 = Tweet(2, 2, time.time(), "sampletext2")
# tweet3 = Tweet(3, 3, time.time(), "sampletext3")
# tweet4 = Tweet(2, 4, time.time(), "sampletext4")
# red_interact.insert_tweet(tweet1)
# red_interact.insert_tweet(tweet2)
# red_interact.insert_tweet(tweet3)
# red_interact.insert_tweet(tweet4)
# timeline = red_interact.get_timeline(2)
# for tweet in timeline:
#     print(str(tweet))
#
# print(red_interact.get_unique_users())

# r.set("next_index", 0)
# print(r.incr("next_index"))
# print(r.incr("next_index"))
# print(type(r.incr("next_index")))
#
# cur_time = time.time()
# for i in range(10000):
#     r.set('key_'+str(i), i)
# end_time = time.time()
#
# print('Inserts per second: ', 10000 / (end_time - cur_time))
# print(type(r.get("key_4")))

interaction = RedisInteract()
strat = "2"

all_users = interaction.get_unique_users()
timeline_time = time.time()
iters = 10000
print('-----------------Retrieving ' + str(iters) + ' Timelines---------------')
for i in range(iters):
    test_user = random.choice(all_users)
    if strat == "1":
        user_timeline = interaction.get_timeline(test_user)
    elif strat == "2":
        user_timeline = interaction.get_timeline_strat2(test_user)
    else:
        user_timeline = interaction.get_timeline(test_user)
timeline_end_time = time.time()
print('Time for ' + str(iters) + ' timelines: ' + str(round((timeline_end_time - timeline_time) / 60, 3)) + ' min')
print('Timelines per second: ', round(iters / (timeline_end_time - timeline_time), 3))
