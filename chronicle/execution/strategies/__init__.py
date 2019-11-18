from chronicle.execution.strategies import duplication

_STRATEGIES_BY_ALIAS = {
    klass.alias(): klass
    for klass in [duplication.SkipCommand, duplication.RestartCommand]
}


def find_strategy_by_alias(name):
    return _STRATEGIES_BY_ALIAS.get(name)
