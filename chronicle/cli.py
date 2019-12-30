import sys
import logging

import click
from dynaconf import settings
from pythonjsonlogger import jsonlogger

from chronicle.application import Application

logging.basicConfig(
    stream=sys.stdout,
    level=settings.get("logger", {}).get("level") or logging.DEBUG,
    format="[%(asctime)s] [%(process)d] %(levelname)s "
    "[%(name)s.%(funcName)s:%(lineno)d] "
    "%(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

LOGGING_FORMAT = {
    "compact": "%(asctime) %(process) %(levelname) %(message)",
    "extended": "%(asctime) %(process) %(levelname) "
    "%(name) %(funcName) %(lineno) %(message)",
}

logger.parent.handlers[0].setFormatter(
    jsonlogger.JsonFormatter(LOGGING_FORMAT[settings["LOGGING"].get("format")])
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
    logger.debug(f"Starting with settings: {settings.as_dict()}")
    if backfill_since == 0:
        backfill_since = None
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
