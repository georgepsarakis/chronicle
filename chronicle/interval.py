from cron_descriptor import (
    Options,
    CasingTypeEnum,
    DescriptionTypeEnum,
    ExpressionDescriptor,
)
from croniter import croniter

import logging

logger = logging.getLogger(__name__)


class CronInterval:
    def __init__(self, expression: str, base_time: int):
        # TODO: add validation
        self._expression = expression
        self._iterator = None
        assert base_time is not None
        self._base_time = base_time
        self._next_run_at = None

    @property
    def base_time(self):
        return self._base_time

    @base_time.setter
    def base_time(self, value):
        self._base_time = value

    @property
    def expression(self):
        return self._expression

    @property
    def iterator(self):
        if self._iterator is None:
            self._iterator = croniter(self.expression, self.base_time)
        return self._iterator

    def is_pending(self, current_time: int):
        return self.next_run_at == int(current_time)

    @property
    def next_run_at(self):
        return self._next_run_at

    def schedule_next(self):
        self._next_run_at = self.iterator.get_next()
        return self._next_run_at

    def parse_expression(self):
        options = Options()
        options.throw_exception_on_parse_error = True
        options.casing_type = CasingTypeEnum.Sentence
        options.use_24hour_time_format = True
        return ExpressionDescriptor(self.expression, options).get_description(
            DescriptionTypeEnum.FULL
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.parse_expression()})"
