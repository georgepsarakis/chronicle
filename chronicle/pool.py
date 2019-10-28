from typing import Union
import logging


from boltons import iterutils
import trio

logger = logging.getLogger(__name__)


class TrioPool:
    def __init__(self, concurrency: Union[int, None]):
        self._concurrency = concurrency
        self._running_jobs = []

    async def execute(self, jobs, *args):
        concurrency = self._concurrency or len(jobs)

        for batch in iterutils.chunked(jobs, concurrency):
            self._running_jobs = []
            async with trio.open_nursery() as nursery:
                for job in batch:
                    self._running_jobs.append(job)
                    nursery.start_soon(job.run, args)
        return jobs

    async def terminate(self):
        return [job.process.terminate() for job in self._running_jobs]

