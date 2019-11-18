from typing import Union
import logging

from boltons import iterutils
import trio

import chronicle.logger as log_helper

logger = logging.getLogger(__name__)


class TrioPool:
    def __init__(self,
                 concurrency: Union[int, None],
                 execution_strategies=None):
        self._concurrency = concurrency
        self._running_commands = set()
        self._execution_strategies = execution_strategies
        self._execution_strategies_initialized = False

    @property
    def execution_strategies(self):
        if self._execution_strategies_initialized:
            return self._execution_strategies
        else:
            if self._execution_strategies:
                strategy_instances = []
                for strategy_class in self._execution_strategies:
                    strategy_instances.append(
                        strategy_class(self._running_commands)
                    )
                self._execution_strategies = strategy_instances
            self._execution_strategies_initialized = True

        return self._execution_strategies

    @property
    def running_commands(self) -> set:
        return self._running_commands

    def _apply_strategies(self, batch):
        if not self.execution_strategies:
            return batch

        final_batch = []
        for job in batch:
            if all(strategy(job) for strategy in self.execution_strategies):
                final_batch.append(job)
        return final_batch

    async def execute(self, commands, *args):
        concurrency = self._concurrency or len(commands)

        for batch in iterutils.chunked(commands, concurrency):
            async with trio.open_nursery() as nursery:
                for command in self._apply_strategies(batch):
                    nursery.start_soon(command.run, args)
                    self.running_commands.add(command)

            for command in batch:
                try:
                    self.running_commands.remove(command)
                except KeyError:
                    log_helper.generate(
                        self.running_commands,
                        tags=['pool', 'removal']
                    )

        return commands

    async def terminate(self):
        return [job.process.terminate() for job in self._running_commands]
