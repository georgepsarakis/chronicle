from chronicle.interval import CronInterval


class TestCronInterval:
    def test_base_time(self, cron_expr_every_minute):
        base_time = 1
        cron_interval = CronInterval(base_time=base_time,
                                     expression=cron_expr_every_minute)
        assert cron_interval.base_time == base_time
