class StrategyBase:
    def __init__(self, running_commands):
        self._running_commands = running_commands

    def already_running(self, command) -> bool:
        return command in self._running_commands

    def process(self, command) -> bool:
        return True

    def __call__(self, command):
        return self.process(command)
