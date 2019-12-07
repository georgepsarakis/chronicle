import sys
import logging

import click
from dynaconf import settings

from chronicle.application import Application

logger = logging.getLogger(__name__)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] [%(process)d] %(levelname)s "
    "[%(name)s.%(funcName)s:%(lineno)d] "
    "%(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--backfill-since",
    default=0,
    show_default=True,
    help="Use this timestamp as a starting point for "
    "scheduling and executing tasks.",
)
def start(backfill_since):
    crontab = Application(settings).create().crontab
    crontab.start(backfill_since)


@cli.command()
@click.option("--interval", default=60, show_default=True)
def pause(interval):
    crontab = Application(settings).create().crontab
    crontab.pause(interval)


@cli.command()
def resume():
    crontab = Application(settings).create().crontab
    crontab.resume()


if __name__ == "__main__":
    cli()
