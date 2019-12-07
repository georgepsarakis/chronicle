import time
import logging

try:
    from redis import StrictRedis

    _ENABLED = True
except ImportError:

    class StrictRedis:
        pass

    _ENABLED = False

logger = logging.getLogger(__name__)


class RedisBackend:
    REDIS_NAMESPACE = "chronicle"

    def __init__(self, connection: StrictRedis):
        self._connection = connection
        self._enabled = _ENABLED

    @property
    def enabled(self):
        return self._enabled

    @property
    def connection(self):
        return self._connection

    @classmethod
    def get_redis_key(cls, *path):
        return ":".join([cls.REDIS_NAMESPACE] + list(path))

    async def store_heartbeat(self):
        self.connection.set(self.get_redis_key("status", "heartbeat"), int(time.time()))

    def pause(self, interval, pattern=".*"):
        pipe = self.connection.pipeline()
        pipe.hset(self.get_redis_key("operations", "pause"), "interval", interval)
        pipe.hset(self.get_redis_key("operations", "pause"), "pattern", pattern)
        pipe.expire(self.get_redis_key("operations", "pause"), interval)
        return pipe.execute()

    def resume(self):
        self.connection.delete(self.get_redis_key("operations", "pause"))

    async def is_paused(self):
        return self.connection.hexists(
            self.get_redis_key("operations", "pause"), "interval"
        )
