# coding: utf-8
from __future__ import annotations
import logging
import os
import sys
import traceback
from subprocess import PIPE, Popen
from threading import Thread
from queue import Queue
from typing import Any
from io import TextIOWrapper

ON_POSIX = 'posix' in sys.builtin_module_names
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def enqueue_output(out: TextIOWrapper, queue: Queue[Any]) -> bool:
    try:
        for line in out.readlines():
            queue.put_nowait(line)
    finally:
        if out and not out.closed:
            try:
                out.close()
                return True
            except:
                return False


def execute(cmd: str,
            *,
            stdout: Any = PIPE,
            bufsize: int = 1,
            with_shell: bool = True,
            cwd: str = os.getcwd(),
            close_fds: bool = ON_POSIX,
            universal_newlines=True,

            # thread dies with the program
            daemon_thread: bool = True,
            timeout: float = 3.0,
            expected_output: bool = False,
            ) -> Any:
    logger.debug(
        f'Trying to execute `{cmd}` with the following list of args -',
        extra={
            'is_daemon_thread': daemon_thread,
            'timeout_sec':      timeout,
        }
    )

    cmd = cmd.strip()
    process: Popen = Popen(cmd,
                           stdout=stdout,
                           bufsize=bufsize,
                           shell=with_shell,
                           cwd=cwd,
                           close_fds=close_fds,
                           universal_newlines=universal_newlines)
    exec_queue: Queue[Any] = Queue()

    exec_thread = Thread(target=enqueue_output, args=(process.stdout, exec_queue,))
    exec_thread.daemon = daemon_thread
    exec_thread.start()

    try:
        if timeout and timeout > 0.0:
            return exec_queue.get(timeout=timeout)
        else:
            return exec_queue.get_nowait()
    except:
        if expected_output:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            logger.exception(f'enqueue_output failed!', exc_info)
            raise
        else:
            return True