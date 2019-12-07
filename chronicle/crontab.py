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
        return f"Invalid value {self.parameter}={self.value}: {self.suggestion}"


class Crontab:
    _jobs = None

    def __init__(
        self,
        max_concurrency=None,
        max_parallel_executions=2,
        execution_strategies=None,
        backend: RedisBackend = None,
    ):
        self._backend = backend
        self._max_concurrency = max_concurrency
        self._max_parallel_executions = max_parallel_executions
        self._start_time = None
        self._execution_strategies = execution_strategies
        self._pool = TrioPool(
            concurrency=self._max_concurrency, execution_strategies=execution_strategies
        )
        self._is_paused = False

        signal.signal(signal.SIGTERM, self._termination_handler)

    def _termination_handler(self, _signum, _frame):
        trio.run(self._pool.terminate)
        sys.exit(0)

    @property
    def max_parallel_executions(self) -> int:
        return self._max_parallel_executions

    @property
    def backend(self):
        return self._backend

    def has_backend(self):
        return self.backend and self.backend.enabled

    def pause(self, interval: int):
        if not self.backend.enabled:
            raise BackendDisabled("The Redis backend is not available")
        return self.backend.pause(interval=interval)

    def resume(self):
        if not self.has_backend():
            raise BackendDisabled("The Redis backend is not available")
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

        trio.run(self._schedule, initial_time)

    def _set_job_base_time(self, initial_time):
        for job in self.get_jobs():
            job.set_initial_time(initial_time)

    async def _schedule(self, initial_time=None):
        scheduler = Scheduler(jobs=self.get_jobs(), initial_time=initial_time)
        self._set_job_base_time(scheduler.clock.current_time)

        while True:
            if self._is_paused:
                scheduler.stop()
            else:
                if scheduler.stopped:
                    scheduler.resume()

            async with trio.open_nursery() as executor_pool:
                max_executions_reached = None

                while max_executions_reached is None or not max_executions_reached:

                    await scheduler.poll()
                    logger.info(
                        log_helper.generate(
                            message=scheduler.queue.qsize(),
                            tags=["scheduler", "queue", "size"],
                        )
                    )

                    if scheduler.queue.empty():
                        continue

                    commands = []
                    for _priority, job in scheduler.flush():
                        if job.command in commands:
                            continue
                        commands.append(job.command)
                        job.interval.schedule_next()

                    extra_environment_vars = self._get_extra_environment_vars(
                        scheduler.clock.current_time
                    )

                    executor_pool.start_soon(
                        self._pool.execute,
                        [command.clone() for command in commands],
                        extra_environment_vars,
                    )

                    # Add a checkpoint
                    await trio.sleep(0)

                    max_executions_reached = (
                        len(executor_pool.child_tasks) == self.max_parallel_executions
                    )

                    logger.info(
                        log_helper.generate(
                            message=len(executor_pool.child_tasks),
                            tags=["executor", "pool", "size"],
                        )
                    )

                await self.check_for_pause()

    @staticmethod
    def _get_extra_environment_vars(current_time):
        return {"CHRONICLE_CRON_TIME": str(current_time), "CHRONICLE_BACKFILL": "false"}
