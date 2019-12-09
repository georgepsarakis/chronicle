import os


MAIN_PROCESS_IDENTIFIER = "COORDINATOR"


def generate(message, task_id=None, pid=None, stream=None, **kwargs):
    pid = pid or os.getpid()
    task_id = task_id or MAIN_PROCESS_IDENTIFIER
    entry = {
        "message": str(message),
        "task_id": task_id,
        "pid": pid,
        "stream": stream,
    }
    entry.update(
        {
            key: list(value) if isinstance(value, (set, tuple)) else value
            for key, value in kwargs.items()
        }
    )
    return entry
