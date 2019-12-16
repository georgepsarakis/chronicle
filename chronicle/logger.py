import os


_MAIN_PROCESS_IDENTIFIER = "COORDINATOR"


def generate(message=None, task_id=None, pid=None, stream=None, **kwargs):
    pid = pid or os.getpid()
    task_id = task_id or _MAIN_PROCESS_IDENTIFIER
    entry = {"message": message, "task_id": task_id, "pid": pid, "stream": stream}
    entry.update(
        {
            key: list(value) if isinstance(value, (set, tuple)) else value
            for key, value in kwargs.items()
        }
    )
    return {
        key: value
        for key, value in filter(lambda item: item[1] is not None, entry.items())
    }
