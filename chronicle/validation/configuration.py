import time

from marshmallow import Schema, fields, validates, validate, ValidationError


class Job(Schema):
    interval = fields.Str(required=True)
    command = fields.Str(required=True)
    bash = fields.Bool(default=False, missing=False)
    environment = fields.Str(default="INHERIT_ALL", missing="INHERIT_ALL")
    timeout = fields.Int(allow_none=True, missing=None)

    @validates("timeout")
    def validate_timeout(self, value):
        if value is None:
            return
        if value < 1:
            raise ValidationError("timeout cannot be less than 1 second")

    @validates("environment")
    def validate_environment(self, value):
        from chronicle.job import CronCommand

        if value not in CronCommand.ENV_OPTIONS_BY_NAME.keys():
            raise ValidationError(
                "Environment inheritance behavior should be one of "
                f"{CronCommand.ENV_OPTIONS_BY_NAME.keys()}"
            )


class Backend(Schema):
    url = fields.URL(schemes=("redis", "rediss"), allow_none=True)


class Scheduler(Schema):
    _MAXIMUM_START_TIME_INTERVAL = 86400

    start_time = fields.Integer(allow_none=True)

    @validates("start_time")
    def validate_start_time(self, value):
        if value is None:
            return
        interval = time.time() - value
        if interval < 0:
            raise ValidationError("Start time cannot be in the future")

        if interval > self._MAXIMUM_START_TIME_INTERVAL:
            raise ValidationError(
                f"Start time cannot be more than "
                f"{self._MAXIMUM_START_TIME_INTERVAL} in the past"
            )


class Strategy(Schema):
    scope = fields.Str(required=True)
    name = fields.Str(required=True)


class Execution(Schema):
    strategies = fields.Nested(Strategy, many=True)


class Logging(Schema):
    format = fields.Str(validate=validate.OneOf(choices=("compact", "extended")))


class Configuration(Schema):
    jobs = fields.Nested(Job, many=True, required=True, data_key="JOBS")
    scheduler = fields.Nested(Scheduler, data_key="SCHEDULER", missing={})
    backend = fields.Nested(Backend, data_key="BACKEND", missing={})
    execution = fields.Nested(Execution, data_key="EXECUTION", missing={})
    logging = fields.Nested(Logging, data_key="LOGGING", missing={})
