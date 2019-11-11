from typing import Union
import logging

from boltons import iterutils
import trio

import chronicle.logger as log_helper

logger = logging.getLogger(__name__)


class TrioPool:
    def __init__(self, concurrency: Union[int, None]):
        self._concurrency = concurrency
        self._running_jobs = set()

    @property
    def running_jobs(self) -> set:
        return self._running_jobs

    async def execute(self, jobs, *args):
        concurrency = self._concurrency or len(jobs)

        for batch in iterutils.chunked(jobs, concurrency):
            async with trio.open_nursery() as nursery:
                for job in batch:
                    if job in self.running_jobs:
                        logger.warning(
                            log_helper.generate(
                                'skipped',
                                str(job),
                                tags=['pool', 'job', 'processing']
                            )
                        )
                        continue
                    nursery.start_soon(job.run, args)
                    logger.info(repr(self.running_jobs))
                    self.running_jobs.add(job)

            for job in batch:
                try:
                    self.running_jobs.remove(job)
                except KeyError:
                    log_helper.generate(
                        self.running_jobs,
                        tags=['pool', 'removal']
                    )

        return jobs

    async def terminate(self):
        return [job.process.terminate() for job in self._running_jobs]
