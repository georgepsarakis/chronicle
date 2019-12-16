from queue import PriorityQueue
import logging
import time

import trio

import chronicle.logger as log_helper

logger = logging.getLogger(__name__)


class Clock:
    _TICK_INTERVAL_SECONDS = 60

    def __init__(self, initial_time: float):
        self._initial_time = self._normalize_time(initial_time)
        self._current_time = self._initial_time

    @staticmethod
    def get_current_timestamp():
        return int(time.time())

    @property
    def now(self):
        return self.get_current_timestamp()

    @property
    def initial_time(self) -> int:
        return self._initial_time

    @property
    def current_time(self) -> int:
        return self._current_time

    def _normalize_time(self, timestamp) -> int:
        timestamp = int(timestamp)
        seconds = timestamp % self._TICK_INTERVAL_SECONDS
        return timestamp - seconds + self._TICK_INTERVAL_SECONDS

    def tick(self):
        self._current_time += self._TICK_INTERVAL_SECONDS
        return self._current_time

    async def __aenter__(self):
        time_remaining = self.current_time - self.now
        if time_remaining >= 1:
            logger.info(
                log_helper.generate(
                    message=time_remaining, tags=["scheduler", "clock", "wait"]
                )
            )
            wait_until = self.now + time_remaining
            while self.now < wait_until:
                await trio.sleep(0.01)
        return self.current_time

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.tick()


class Scheduler:
    _MINIMUM_TICK_INTERVAL = 0.1

    def __init__(self, jobs, initial_time):
        self._jobs = jobs
        self._queue = PriorityQueue()
        self._initial_time = initial_time
        self._clock = None
        self._stopped = False

    @property
    def stopped(self):
        return self._stopped

    @property
    def clock(self):
        if self._clock is None:
            self._clock = Clock(initial_time=self.initial_time)
        return self._clock

    @property
    def queue(self):
        return self._queue

    @property
    def initial_time(self):
        return self._initial_time

    def flush(self):
        items = []
        while not self._queue.empty():
            items.append(self._queue.get())
        return items

    def stop(self):
        self._stopped = True

    def resume(self):
        self._stopped = False

    async def poll(self):
        async with self.clock as current_time:
            if self._stopped:
                logger.info(
                    log_helper.generate(
                        message="paused",
                        tags=["scheduler", "queue", "freeze"],
                        status="PAUSED",
                        schedule_time=current_time,
                    )
                )
                return
            for job in self._jobs:
                logger.info(
                    log_helper.generate(
                        task_id=job.identifier,
                        tags=["scheduler", "queue", "check"],
                        schedule_time=current_time,
                        remaining_time=job.time_left_for_next_run(self.clock.now),
                    )
                )
                if job.is_pending(current_time):
                    logger.info(
                        log_helper.generate(
                            task_id=job.identifier,
                            tags=["scheduler", "queue", "put"],
                            schedule_time=current_time,
                        )
                    )
                    self.queue.put((job.priority, job))
        return self.queue.qsize()
