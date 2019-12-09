import os
from chronicle import logger


class TestLogger:
    def test_generate(self):
        assert logger.generate("test message") == \
               {
                   "message": "test message",
                   "task_id": logger.MAIN_PROCESS_IDENTIFIER,
                   "pid": os.getpid(),
                   "stream": None,
               }
