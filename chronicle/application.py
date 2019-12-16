import logging

import jmespath
from dynaconf import settings

from chronicle.job import Job
from chronicle.crontab import Crontab, AlreadyConfigured
from chronicle.backend import RedisBackend, StrictRedis
from chronicle.execution.strategies import find_strategy_by_alias

logger = logging.getLogger(__name__)


class Application:
    def __init__(self, configuration):
        self._configuration = configuration
        self._is_configured = False
        self._crontab = None

    @property
    def crontab(self) -> Crontab:
        return self._crontab

    def create(self):
        configuration = self._configure()
        logger.info(f"Creating crontab with configuration: {configuration}")
        self._crontab = Crontab(**configuration)
        return self

    def _get_duplication_strategy(self):
        selected_strategy = jmespath.search(
            "execution.strategies[?scope==`duplication`] | [0].name",
            self._configuration,
        )
        return find_strategy_by_alias(f"duplication:{selected_strategy}")

    def _get_backend_url(self):
        return jmespath.search("backend.url", self._configuration)

    @staticmethod
    def _get_jobs():
        return settings.JOBS

    def _configure(self):
        if self._is_configured:
            raise AlreadyConfigured

        cron_jobs = []
        for parameters in self._get_jobs():
            cron_job = Job.from_dict(parameters)
            logger.info(parameters)
            cron_jobs.append(cron_job)
        Crontab.setup(cron_jobs)

        self._is_configured = True
        backend_url = self._get_backend_url()
        if backend_url:
            backend = RedisBackend(StrictRedis.from_url(backend_url))
        else:
            backend = None
        return {
            "execution_strategies": [self._get_duplication_strategy()],
            "backend": backend,
        }
