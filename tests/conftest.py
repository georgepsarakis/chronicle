import pytest


@pytest.fixture
def cron_expr_every_minute():
    return '* * * * *'
