from collections.abc import Mapping
from functools import total_ordering
import json
import logging
import math
import os
from uuid import uuid4
import subprocess
import shlex
import typing

import trio


from chronicle.interval import CronInterval
from chronicle.scheduler import Clock

import chronicle.logger as log_helper

logger = logging.getLogger(__name__)


class TaskAlreadyRegistered(Exception):
    pass


class TaskConfigurationError(Exception):
    pass


class ImmutableMapping(Mapping):
    def __init__(self, **kwargs):
        self._items = kwargs

    def __getitem__(self, key):
        return self._items[key]

    def __len__(self):
        return len(self._items)

    def __iter__(self):
        return iter(self._items)


class CronCommand:
    ENV_INHERIT_ALL = object()
    ENV_INHERIT_WHITELISTED_ONLY = object()
    ENV_OPTIONS_BY_NAME = ImmutableMapping(
        INHERIT_ALL=ENV_INHERIT_ALL,
        INHERIT_WHITELISTED_ONLY=ENV_INHERIT_WHITELISTED_ONLY
    )

    WHITELISTED_ENVIRONMENT_VARS = (
        'PATH',
        'LANG',
        'HOME',
        'USER',
        'LC_NAME',
        'LC_TIME',
        'PWD'
    )
    FLUSH_STREAM_TIMEOUT = 30

    def __init__(self, command, **options):
        self._options = options
        self._command = command
        self._timeout = options.get('timeout', math.inf) or math.inf
        self._shell = options.get('shell', True)
        self._use_bash = options.get('bash', False)

        if self._use_bash and not self._shell:
            raise TaskConfigurationError('Invalid shell configuration')

        self._environment = options.get('environment',
                                        self.ENV_INHERIT_ALL)

        if not self._shell:
            command = shlex.split(command)

        self._command = command
        self._identifier = str(uuid4())
        self._started_at = None
        self._process = None

    def clone(self):
        new_instance = self.__class__(self._command, **self._options)
        new_instance._identifier = self.identifier
        return new_instance

    def _get_base_environment(self):
        environment = os.environ.copy()
        if self.environment is self.ENV_INHERIT_WHITELISTED_ONLY:
            environment = {
                variable: value
                for variable, value in environment.items()
                if variable in self.WHITELISTED_ENVIRONMENT_VARS
            }
        return environment

    def as_tuple(self) -> tuple:
        return self._get_base_environment(), self._get_executable()

    @property
    def environment(self):
        return self._environment

    @property
    def shell(self):
        return self._shell

    @property
    def command(self):
        return self._command

    @property
    def timeout(self):
        return self._timeout

    @property
    def process(self):
        return self._process

    @property
    def identifier(self):
        return self._identifier

    def _get_executable(self):
        if self._use_bash:
            return f"/bin/bash -c {shlex.quote(self.command)}"

        return self.command

    async def run(self, additional_environment=None):
        environment = os.environ.copy()
        if self.environment is self.ENV_INHERIT_WHITELISTED_ONLY:
            environment = {
                variable: value
                for variable, value in environment.items()
                if variable in self.WHITELISTED_ENVIRONMENT_VARS
            }
        environment.update(additional_environment or {})

        with trio.move_on_after(self.timeout) as cancel_scope:
            self._process = await trio.open_process(
                self._get_executable(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=self._shell,
                env=environment
            )

            logger.info(
                log_helper.generate(
                    message=self._get_executable(),
                    task_id=self.identifier,
                    pid=self.process.pid,
                    stream=None,
                    tags=['command', 'execution'],
                    status='PENDING'
                )
            )
            self._started_at = Clock.get_current_timestamp()
            await self.show_progress()

        if cancel_scope.cancelled_caught:
            logger.error(
                log_helper.generate(
                    f'timeout since {self._started_at}',
                    task_id=self.identifier,
                    pid=self.process.pid,
                    tags=['command', 'execution'],
                    status='CANCELLED'
                )
            )

            with trio.move_on_after(self.FLUSH_STREAM_TIMEOUT):
                await self.flush_streams(final=True)

    async def flush_streams(self, final=False):
        streams_active = True
        while streams_active:
            streams_active = False
            stdout = await self.process.stdout.receive_some()
            if stdout:
                streams_active = True
                logger.info(
                    log_helper.generate(
                        stream='STDOUT',
                        message=stdout.decode(),
                        task_id=self.identifier,
                        pid=self.process.pid
                    )
                )

            stderr = await self.process.stderr.receive_some()
            if stderr:
                streams_active = True
                logger.info(
                    log_helper.generate(
                        stream='STDERR',
                        message=stderr.decode(),
                        task_id=self.identifier,
                        pid=self.process.pid
                    )
                )
            if not final:
                return

    async def show_progress(self):
        while self.process is None:
            trio.sleep(0.001)

        logger.info(
            log_helper.generate(
                message=self._get_executable(),
                task_id=self._identifier,
                pid=self.process.pid,
                tags=['command', 'execution'],
                status='STARTED'
            )
        )

        while self.process.poll() is None:
            await self.flush_streams()
        await self.flush_streams(final=True)

        successful = self.process.returncode == 0

        logger.info(
            log_helper.generate(
                message=self._get_executable(),
                task_id=self.identifier,
                pid=self.process.pid,
                returncode=self.process.returncode,
                status='SUCCESS' if successful else 'FAILURE'
            )
        )

    def __repr__(self):
        return f'CronCommand({self.command})'

    def __hash__(self):
        return hash(json.dumps(self.as_tuple()))

    def __eq__(self, other):
        return self.as_tuple() == other.as_tuple()


@total_ordering
class Job:
    def __init__(self,
                 interval: typing.Union[str, CronInterval],
                 command: CronCommand):
        self._interval = interval
        self._command = command
        self._initial_time = None

    def set_initial_time(self, value: int):
        self._initial_time = value
        return value

    @classmethod
    def from_dict(cls, parameters):
        interval = parameters['interval']
        parameters['environment'] = \
            CronCommand.ENV_OPTIONS_BY_NAME[parameters['environment']],
        command = CronCommand(
            **{
                key: value
                for key, value in parameters.items()
                if key != 'interval'
            }
        )
        return cls(interval=interval, command=command)

    @property
    def command(self):
        return self._command

    @property
    def interval(self):
        if not isinstance(self._interval, CronInterval):
            self._interval = CronInterval(self._interval, self._initial_time)
        return self._interval

    @property
    def priority(self):
        return self.interval.next_run_at

    def __hash__(self):
        hash(str(self.command) + str(self.interval))

    def is_pending(self, now: int):
        return self.interval.is_pending(now)

    def time_left_for_next_run(self, now):
        return self.interval.next_run_at - now

    def __lt__(self, other):
        return self.interval.next_run_at < other.interval.next_run_at

    def __eq__(self, other):
        return self.interval.next_run_at == other.interval.next_run_at

    def __repr__(self):
        return f'{self.command} at {self.interval}'
