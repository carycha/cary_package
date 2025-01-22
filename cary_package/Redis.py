from dotenv import dotenv_values
import redis
import json
import hashlib


class RedisModel:
    def __init__(self, secret_dict):

        self.redis_cache = redis.Redis(
            host=secret_dict["host"],
            port=secret_dict["port"],
            password=secret_dict["password"],
            decode_responses=True,
        )

    def get_hash(self, word):
        word = json.dumps(word, ensure_ascii=False)
        data_sha = hashlib.sha256(word.encode("utf-8")).hexdigest()
        return data_sha

    def get_cache(self, key):
        redis_cache = self.redis_cache
        # key = json.dumps(key, ensure_ascii=False)
        key_hash = self.get_hash(key)
        cache_result = redis_cache.get(key_hash)
        if cache_result:
            return json.loads(cache_result)
        else:
            return cache_result

    def set_cache(self, key, value, expire_time=86400):
        redis_cache = self.redis_cache
        # key = json.dumps(key, ensure_ascii=False)
        key_hash = self.get_hash(key)
        value = json.dumps(value, ensure_ascii=False)
        redis_cache.set(key_hash, value, ex=expire_time)
        # redis_cache.expire(key, expire_time)
        return
