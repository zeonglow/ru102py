# Uncomment for Challenge #7
import random
import time
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""

        # Add the current hit to the value
        pipeline = self.redis.pipeline(transaction=False)
        key = self.key_schema.sliding_window_rate_limiter_key(
            name,
            int(self.window_size_ms),
            self.max_hits
        )
        ts = int(time.monotonic() * 1000)
        value = f"{ts}-{random.randint(0, self.max_hits)}"
        pipeline.zadd(key, {value: ts})
        pipeline.zremrangebyscore(key, 0, ts - self.window_size_ms)
        pipeline.zcard(key)
        _, _2, count = pipeline.execute()

        if count > self.max_hits:
            raise RateLimitExceededException
