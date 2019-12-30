import logging

import chronicle.logger as log_helper

logger = logging.getLogger(__name__)


class StrategyBase:
    def __init__(self, process_list):
        self._process_list = process_list

    @classmethod
    def alias(cls):
        raise NotImplementedError

    def already_running(self, command) -> bool:
        return command in self._process_list

    def process(self, command) -> bool:
        return True

    def __call__(self, command):
        return self.process(command)


class SkipCommand(StrategyBase):
    @classmethod
    def alias(cls):
        return "duplication:skip"

    def process(self, command):
        if self.already_running(command):
            logger.warning(
                log_helper.generate(
                    "skipped",
                    str(command),
                    tags=["duplicate_command", "job", "filtering"],
                )
            )
            return False
        return True


class RestartCommand(StrategyBase):
    @classmethod
    def alias(cls):
        return "duplication:restart"

    def _find_running_instance(self, command):
        for job_instance in self._process_list:
            if command == job_instance:
                return job_instance

    def process(self, command):
        if self.already_running(command):
            running_instance = self._find_running_instance(command)
            running_instance.process.terminate()
            logger.warning(
                log_helper.generate(
                    "terminated",
                    str(command),
                    tags=["duplicate_command", "job", "filtering"],
                )
            )
        return True
