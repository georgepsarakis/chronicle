import sys
import logging

import click

from chronicle.job import Job
from chronicle.crontab import Crontab
from chronicle.backend import RedisBackend, StrictRedis
from chronicle.execution.strategies.duplication import \
    SkipCommand, RestartCommand

logger = logging.getLogger(__name__)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] [%(process)d] %(levelname)s "
           "[%(name)s.%(funcName)s:%(lineno)d] "
           "%(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)

configuration = {
    'jobs': [{
        'interval': '*/2 * * * *',
        'command': 'for i in 1 2 3 4 5 6 7 8 9 10; do date; sleep 15; done',
        'bash': True,
        'environment': 'INHERIT_ALL',
        'timeout': None
    # }, {
    #     'interval': '*/1 * * * *',
    #     'command': 'date && env',
    #     'bash': True,
    #     'environment': 'INHERIT_WHITELISTED_ONLY'
    }, #{
    #     'interval': '* * * * *',
    #     'command': 'env | grep CHRONICLE',
    #     'bash': True,
    #     'environment': 'INHERIT_WHITELISTED_ONLY'
    # }, # {
        # 'interval': '*/9 * * * *',
        # 'command': 'ls',
        # 'bash': True,
        # 'environment': 'INHERIT_WHITELISTED_ONLY'
#    }
],
    'backend': {
        'url': 'redis://localhost:6379'
    },
    'execution': {
        'strategies': {
            'duplication': 'skip'
        }
    }
}

Crontab.setup(
    jobs=[
        Job.from_dict(parameters)
        for parameters in configuration['jobs']
    ]
)


def _integer_option(_ctx, _param, value):
    if value is not None:
        return int(value)
    return value


def _get_nested_configuration(config, *path):
    c = config
    for segment in path:
        c = c.get(segment, {})
    return c

def _initialize_crontab(conf):
    strategies = []
    duplication_strategy = _get_nested_configuration(
        conf,
        'execution',
        'strategies',
        'duplication'
    )
    if duplication_strategy:
        if duplication_strategy == 'skip':
            strategies.append(SkipCommand)
        elif duplication_strategy == 'restart':
            strategies.append(RestartCommand)

    parameters = {}
    if 'backend' in conf:
        parameters['backend'] = \
            RedisBackend(StrictRedis.from_url(conf['backend'].get('url')))

    if strategies:
        parameters['execution_strategies'] = strategies

    return Crontab(**parameters)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--backfill-since',
              callback=_integer_option,
              default=None,
              help='Use this timestamp as a starting point for '
                   'scheduling and executing tasks.')
def start(backfill_since):
    crontab = _initialize_crontab(configuration)
    crontab.start(backfill_since)


@cli.command()
@click.option('--interval', callback=_integer_option)
@click.option('--pattern', default=None)
def pause(interval, pattern):
    crontab = _initialize_crontab(configuration)
    crontab.pause(interval)


@cli.command()
def resume():
    crontab = _initialize_crontab(configuration)
    crontab.resume()


if __name__ == '__main__':
    cli()
