from collections.abc import MutableSet
from typing import Union
import logging

from boltons import iterutils
import trio

import chronicle.logger as log_helper

logger = logging.getLogger(__name__)


class StrategyHandler:
    def __init__(self, selected_strategies, process_list):
        self._selected_strategies = selected_strategies or []
        self._prepared_strategies = []
        self._process_list = process_list

    @property
    def strategies(self):
        if not self._prepared_strategies:
            self._prepared_strategies = [
                strategy_class(self._process_list)
                for strategy_class in self._selected_strategies
            ]
        return self._prepared_strategies

    def apply(self, command):
        for strategy in self.strategies:
            if not strategy(command):
                return False
        return True


class ProcessList(MutableSet):
    def __init__(self):
        self._running_commands = set()

    @property
    def commands(self):
        return self._running_commands.copy()

    def update(self, commands):
        self._running_commands.update(commands)

    def add(self, command):
        self._running_commands.add(command)

    def remove(self, command):
        self._running_commands.remove(command)

    def discard(self, command):
        self._running_commands.discard(command)

    def __contains__(self, command):
        return command in self._running_commands

    def __len__(self):
        return len(self._running_commands)

    def __iter__(self):
        for command in self._running_commands:
            yield command


class TrioPool:
    def __init__(self, concurrency: Union[int, None], execution_strategies=None):
        self._concurrency = concurrency
        self._process_list = ProcessList()
        self._execution_strategies = execution_strategies
        self._strategy_handler = None

    @property
    def strategy_handler(self):
        if self._strategy_handler is None:
            self._strategy_handler = StrategyHandler(
                self._execution_strategies, self.processlist
            )
        return self._strategy_handler

    @property
    def strategies(self):
        return self.strategy_handler.strategies

    @property
    def processlist(self) -> ProcessList:
        return self._process_list

    async def execute(self, commands, *args):
        for batch in self._generate_batches(commands):
            accepted_commands = set(filter(self.strategy_handler.apply, batch))
            self.processlist.update(accepted_commands)

            async with trio.open_nursery() as nursery:
                for command in accepted_commands:
                    nursery.start_soon(command.run, *args)

            await self._mark_as_completed(accepted_commands)
        return commands

    async def terminate(self):
        return [job.process.terminate() for job in self.processlist.commands]

    def _generate_batches(self, commands):
        concurrency = self._concurrency or len(commands)
        yield from iterutils.chunked(commands, concurrency)

    async def _mark_as_completed(self, commands):
        for command in commands:
            try:
                self.processlist.remove(command)
            except KeyError:
                log_helper.generate(self.processlist.commands, tags=["pool", "removal"])
