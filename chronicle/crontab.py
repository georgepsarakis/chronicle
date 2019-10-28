import logging
import signal
import sys

import trio

from chronicle.scheduler import Scheduler, Clock
from chronicle.pool import TrioPool
import chronicle.logger as log_helper
from chronicle.backend import RedisBackend

logger = logging.getLogger(__name__)


class AlreadyConfigured(Exception):
    pass


class AlreadyStarted(Exception):
    pass


class BackendDisabled(Exception):
    pass


class InvalidConfiguration(Exception):
    def __init__(self, parameter, value, suggestion):
        super(InvalidConfiguration, self).__init__()
        self.parameter = parameter
        self.value = value
        self.suggestion = suggestion

    def __str__(self):
        return f'Invalid value {self.parameter}={self.value}: ' \
               f'{self.suggestion}'


class Crontab:
    _jobs = None

    def __init__(self, max_concurrency=None, backend: RedisBackend=None):
        self._backend = backend
        self._max_concurrency = max_concurrency
        self._start_time = None
        self._pool = TrioPool(concurrency=self._max_concurrency)
        self._is_paused = False
        signal.signal(signal.SIGTERM, self._termination_handler)

    def _termination_handler(self, _signum, _frame):
        trio.run(self._pool.terminate)
        sys.exit(0)

    @property
    def backend(self):
        return self._backend

    def has_backend(self):
        return self.backend and self.backend.enabled

    def pause(self, interval: int):
        if not self.backend.enabled:
            raise BackendDisabled('The Redis backend is not available')
        return self.backend.pause(interval=interval)

    def resume(self):
        if not self.has_backend():
            raise BackendDisabled('The Redis backend is not available')
        return self.backend.resume()

    async def check_for_pause(self):
        if not self.has_backend():
            return False
        self._is_paused = await self.backend.is_paused()
        return self._is_paused

    @classmethod
    def setup(cls, jobs):
        if cls.get_jobs():
            raise AlreadyConfigured(cls.get_jobs())
        cls._jobs = jobs

    @classmethod
    def get_jobs(cls):
        return cls._jobs

    @property
    def start_time(self):
        return self._start_time

    def start(self, initial_time=None):
        if self._start_time is not None:
            raise AlreadyStarted

        self._start_time = Clock.get_current_timestamp()
        if initial_time is None:
            initial_time = self._start_time

        self._set_job_base_time(initial_time)
        trio.run(self._schedule, initial_time)

    def _set_job_base_time(self, initial_time):
        for job in self.get_jobs():
            job.interval.base_time = initial_time

    async def _schedule(self, initial_time=None):
        scheduler = Scheduler(jobs=self.get_jobs(), initial_time=initial_time)

        while True:
            if self._is_paused:
                scheduler.stop()
            else:
                if scheduler.stopped:
                    scheduler.resume()

            await scheduler.poll()
            logger.info(
                log_helper.generate(
                    message=scheduler.queue.qsize(),
                    tags=['scheduler', 'queue', 'size']
                )
            )

            if scheduler.queue.empty():
                continue

            await self._pool.execute(
                [item[1].command for item in scheduler.flush()],
                self._get_extra_environment_vars(scheduler.clock.current_time)
            )
            await self.check_for_pause()

    @staticmethod
    def _get_extra_environment_vars(current_time):
        return dict(
                    CHRONICLE_CRON_TIME=str(current_time),
                    CHRONICLE_BACKFILL='false'
                )
