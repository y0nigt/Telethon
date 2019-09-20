# coding: utf-8
from __future__ import annotations

import logging
import math
from collections import deque

from dataclasses import dataclass, \
    field
from decimal import Decimal

from typing import Tuple, \
    Union, \
    ClassVar, \
    Optional, \
    Callable

import threading

import time

import asyncio


mod_logger = logging.getLogger(__name__)


class RateLimiter(object):
    __slots__ = [
        'disabled',
        'window_size_sec',
        'max_burst_limit',
        '__calls_q__',
        '__lock__',
        '__alock__',
        '__timeit__',
        '__loop__',
    ]

    MIN_WINDOW_SIZE_SEC: ClassVar[float] = 1.017
    MIN_BURST_LIMIT: ClassVar[float] = 1.0

    def __init__(
            self,
            burst_limit: Union[int, float] = MIN_BURST_LIMIT,
            window_size_sec: Optional[Union[int, float]] = MIN_WINDOW_SIZE_SEC,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        super().__init__()
        self.__loop__ = loop or asyncio.get_event_loop()

        max_burst_limit = Decimal(
            max(
                [
                    math.floor(burst_limit) - 1,
                    RateLimiter.MIN_BURST_LIMIT
                ]
            )
        )

        self.max_burst_limit = Decimal(int(max_burst_limit))
        self.window_size_sec = Decimal(float(window_size_sec))

        if self.window_size_sec <= Decimal(0):
            self.disabled = True
            return

        self.__calls_q__ = deque()
        self.__lock__ = threading.Lock()
        self.__alock__ = asyncio.Lock(loop=self.__loop__)
        self.__timeit__ = time.perf_counter

    def __enter__(self):
        mod_logger.debug(f'START __enter__')
        with self.__lock__:
            mod_logger.debug(f'__enter__: sync lock acquired')

            if len(self.__calls_q__) >= self.max_burst_limit:
                mod_logger.debug(f'__enter__: queue({len(self.__calls_q__)}) >= max_burst_limit({self.max_burst_limit})')
                sleep_result = self.sleep_seconds()
                if sleep_result > Decimal(0.0001):
                    mod_logger.debug(f'__enter__: SLEEPING for {sleep_result}sec')
                    time.sleep(float(sleep_result))
                else:
                    mod_logger.debug(f'__enter__: NOT SLEEPING for {sleep_result}sec')
            else:
                mod_logger.debug(f'__enter__: NOT SLEEPING')

        mod_logger.debug(f'END __enter__: sync lock released')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        now = Decimal(self.__timeit__())
        mod_logger.debug(f'START __exit__', {'exc_val': exc_val})

        with self.__lock__:
            mod_logger.debug(f'__exit__: sync lock acquired')
            self.exit_shared(now, '__exit__')

        mod_logger.debug(f'END __exit__: sync lock released')

    async def __aenter__(self):
        mod_logger.debug(f'START __aenter__')
        async with self.__alock__:
            mod_logger.debug(f'__aenter__: async lock acquired')
            if len(self.__calls_q__) >= self.max_burst_limit:
                mod_logger.debug(
                    f'__enter__: queue({len(self.__calls_q__)}) >= max_burst_limit({self.max_burst_limit})')
                sleep_result = self.sleep_seconds()
                if sleep_result > Decimal(0.0001):
                    print(f'Sleeping for {sleep_result}sec')
                    await asyncio.sleep(float(sleep_result), loop=self.__loop__)
                else:
                    mod_logger.debug(f'__enter__: NOT SLEEPING for {sleep_result}sec')
            else:
                mod_logger.debug(f'__aenter__: NOT SLEEPING')

        mod_logger.debug(f'END __aenter__: async lock released')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        now = Decimal(self.__timeit__())
        mod_logger.debug(f'START __aexit__', {'exc_val': exc_val})
        async with self.__alock__:
            mod_logger.debug(f'__aexit__: async lock acquired')
            self.exit_shared(now, '__aexit__')

        mod_logger.debug(f'END __aexit__: async lock released')

    def exit_shared(self, now, callsite_name):
        if len(self.__calls_q__) > 0:
            mod_logger.debug(f'{callsite_name}: queue length({len(self.__calls_q__)}) > 0')
            if now - self.window_size_sec >= self.__calls_q__[-1]:
                mod_logger.debug(f'{callsite_name}: clearing queue - last in queue not in window')
                self.__calls_q__.clear()
        self.__calls_q__.append(round(now, 4))

        while (self.__calls_q__[-1] - self.__calls_q__[0]) >= self.window_size_sec:
            ts = self.__calls_q__.popleft()
            mod_logger.debug(f'{callsite_name}: popped-left from queue - {ts}')

    def __await__(self):
        mod_logger.debug(f'START __await__: before sleep of 0')
        return asyncio.sleep(0, loop=self.__loop__).__await__()

    def sleep_seconds(self):
        res = min([
            round(Decimal(self.window_size_sec + self.__calls_q__[0] - self.__calls_q__[-1]), 4),
            self.window_size_sec
        ])

        mod_logger.debug({
            'res': res,
            'window_size_sec': round(self.window_size_sec, 4),
            'queue': repr(list(self.__calls_q__)),
        })

        return res


@dataclass
class ActionRateLimitDefinition(object):
    namespace: str = field(default='generic')
    action: str = field(default=None)
    burst_limit: float = field(default=None)
    window_size_sec: float = field(default=None)

    def generate(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        if not loop:
            loop = asyncio.get_event_loop()

        return RateLimiter(
            burst_limit=self.burst_limit,
            window_size_sec=self.window_size_sec,
            loop=loop
        )


# Using chained bot API limits -
#
# 1. `send_message` to group - 20 messages / 60 seconds
# 2. `send_message` to user  - 30 messages / 1s seconds

def get_rate_limiters() -> Tuple[RateLimiter, RateLimiter]:
    user_message_definition = ActionRateLimitDefinition(
            namespace='api_action',
            action='send_message--user',
            burst_limit=30,
            window_size_sec=1.017
        )

    group_message_definition = ActionRateLimitDefinition(
        namespace='api_action',
        action='send_message--group',
        burst_limit=20,
        window_size_sec=61.02
    )

    return group_message_definition, user_message_definition
