from functools import wraps
import logging
import time
from uuid import uuid4

from transitions import Machine
from marshmallow import Schema, fields, validate
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


# TODO: deprecate in favor of (or move to) marshmallow schema validation
class InvalidConfiguration(Exception):
    def __init__(self, parameter, value, suggestion):
        super(InvalidConfiguration, self).__init__()
        self.parameter = parameter
        self.value = value
        self.suggestion = suggestion

    def __str__(self):
        return f"Invalid value {self.parameter}={self.value}: {self.suggestion}"


class PollingLoop:
    def __init__(self, interval, status_reporter, conditional):
        self._interval = interval
        self._status_reporter = status_reporter
        self._conditional = conditional

    def __enter__(self):
        elapsed_time = 0
        while not self._conditional(elapsed_time):
            time.sleep(self._interval)
            elapsed_time += self._interval
            self._status_reporter(elapsed_time)


class CrontabOptions(Schema):
    dry_run = fields.Bool(default=False, missing=False)
    max_concurrency = fields.Integer(
        validate=validate.Range(min=1),
        allow_none=True,
        default=None,
        missing=None
    )
    max_parallel_executions = fields.Integer(
        validate=validate.Range(min=2),
        default=2,
        missing=2
    )


class Crontab:
    _jobs = None

    def __init__(
        self,
        execution_strategies=None,
        backend: RedisBackend = None,
        **options
    ):
        self._backend = backend
        self._start_time = None
        self._execution_strategies = execution_strategies
        self._options = CrontabOptions().load(options or {})
        self._pool = None
        self._state = CrontabState(self)
        self._scheduler = None

    class Decorators:
        def requires_backend(fn):
            @wraps(fn)
            def wrapper(self, *args, **kwargs):
                if self.has_backend():
                    return fn(self, *args, **kwargs)
                raise BackendDisabled("The Redis backend is not available")
            return wrapper

    @property
    def run_state(self):
        return self._state

    @property
    def scheduler(self) -> Scheduler:
        return self._scheduler

    @property
    def pool(self):
        if self._pool is None:
            self._pool = TrioPool(
                concurrency=self._options["max_concurrency"],
                execution_strategies=self._execution_strategies
            )
        return self._pool

    def stop(self, warm=False):
        self.change_state_to_stopping()
        if not warm:
            logger.info(
                log_helper.generate(
                    message="Terminating all active subprocesses in pool",
                    shutdown="cold"
                )
            )
            trio.run(self._pool.terminate)
            logger.info(
                log_helper.generate(
                    message="Running processes terminated",
                    shutdown="cold"
                )
            )
        else:
            waiter = PollingLoop(
                interval=1,
                status_reporter=
                lambda elapsed_time:
                logger.info(
                    log_helper.generate(
                        message=
                        f"Waiting for all active subprocesses in pool to complete",
                        elapsed_time=elapsed_time,
                        shutdown="warm"
                    )
                )
            )
            with waiter:
                pass
            logger.info(
                log_helper.generate(
                    message="All subprocesses completed",
                    shutdown="warm"
                )
            )
        self.change_state_to_stopped()

    @property
    def max_parallel_executions(self) -> int:
        return self._options["max_parallel_executions"]

    @property
    def backend(self):
        return self._backend

    def has_backend(self):
        return self.backend and self.backend.enabled

    @Decorators.requires_backend
    def pause(self, interval: int):
        return self.backend.pause(interval=interval)

    @Decorators.requires_backend
    def resume(self):
        return self.backend.resume()

    async def check_for_pause(self):
        if not self.has_backend():
            return False
        if await self.backend.is_paused():
            self.change_state_to_paused()
        return self.run_state.machine.is_paused()

    # TODO: provide a better way to configure jobs
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

    def start(self, initial_time=None, single_cycle=False):
        if self._start_time is not None:
            raise AlreadyStarted

        if initial_time == 0:
            raise InvalidConfiguration("Initial time cannot be zero")

        self._start_time = Clock.get_current_timestamp()
        if initial_time is None:
            initial_time = self._start_time

        trio.run(self._schedule, initial_time, single_cycle)

    async def _poll(self):
        queued_jobs_count = await self.scheduler.poll()
        logger.info(
            log_helper.generate(
                message=queued_jobs_count, tags=["scheduler", "queue", "size"]
            )
        )
        return not self.scheduler.queue.empty()

    async def _execute_commands(self, nursery):
        execution_cycle_id = str(uuid4())
        extra_environment_vars = self._get_extra_environment_vars(
            self.scheduler.clock.current_time, execution_cycle_id
        )

        commands = self._get_pending_commands()

        if not commands:
            return

        nursery.start_soon(
            self.pool.execute,
            [command.clone() for command in commands],
            extra_environment_vars,
        )

        # Add a checkpoint
        await trio.sleep(0)

    def _get_pending_commands(self):
        commands = []
        for _priority, job in self.scheduler.flush():
            job.interval.schedule_next()
            if job.command in commands:
                continue
            commands.append(job.command)
        return commands

    async def _schedule(self, initial_time, single_cycle):
        self._scheduler = Scheduler(jobs=self.get_jobs(), initial_time=initial_time)
        self._set_job_base_time()

        while True:
            async with trio.open_nursery() as executor_pool:
                while True:
                    await self.check_for_pause()
                    self._handle_pause()

                    if not await self._poll():
                        continue

                    await self._execute_commands(executor_pool)

                    if self._check_parallel_executions_count(executor_pool) \
                            or self.is_stopping():
                        break

            if single_cycle or self.is_stopping():
                logger.info(
                    log_helper.generate(message='Single cycle selected - exiting')
                )
                break

    def _check_parallel_executions_count(self, nursery):
        max_parallel_executions_reached = (
            len(nursery.child_tasks) == self.max_parallel_executions
        )
        logger.info(
            log_helper.generate(
                message=len(nursery.child_tasks),
                tags=["executor", "pool", "size"],
                max_parallel_executions=self.max_parallel_executions,
                max_parallel_executions_reached=max_parallel_executions_reached,
            )
        )
        return max_parallel_executions_reached

    def _set_job_base_time(self):
        initial_time = self.scheduler.clock.current_time
        for job in self.get_jobs():
            job.set_initial_time(initial_time)

    def _handle_pause(self):
        if self.is_paused():
            self.scheduler.stop()
        else:
            if self.scheduler.stopped:
                self.scheduler.resume()

    @staticmethod
    def _get_extra_environment_vars(current_time, execution_cycle_id):
        return {
            "CHRONICLE_CRON_TIME": str(current_time),
            "CHRONICLE_BACKFILL": "false",
            "CHRONICLE_EXECUTION_CYCLE_ID": execution_cycle_id,
            "CHRONICLE_TASK_ID": lambda command: command.identifier,
        }


class CrontabState:
    _states = (
        'initialized',
        'starting', 'started',
        'stopping', 'stopped',
        'paused', 'resuming',
    )

    def __init__(self, crontab: Crontab):
        self._state_machine = Machine(crontab,
                                      states=self._states,
                                      initial="initialized")
        self._state_machine.add_transition(
            "change_state_to_starting",
            "initialized",
            "starting"
        )
        self._state_machine.add_transition(
            "change_state_to_paused",
            "started",
            "paused"
        )
        self._state_machine.on_enter_paused('_handle_pause')
        self._state_machine.add_transition(
            "change_state_to_stopping",
            "started",
            "stopping"
        )
        self._state_machine.add_transition(
            "change_state_to_stopping",
            "paused",
            "stopping"
        )

    @property
    def machine(self) -> Machine:
        return self._state_machine
