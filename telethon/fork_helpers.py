# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import os
import sys
from typing import Union, \
    ClassVar
import random
from operator import add, \
    sub
from decimal import (
    Decimal,
)

import logging

import numpy as np

HALF_PIE = float(round(Decimal(math.pi) / Decimal(2), 3))

logger = logging.getLogger(__name__)

__current_dir__: str = os.path.dirname(
    os.path.abspath(__file__)
)

_MATH_BINARY_OPS = [add, sub]

_TypeNum = Union[int, float, Decimal]

_SYSRNG = random.SystemRandom()

_choice = _SYSRNG.choice
_randint = _SYSRNG.randint
_uniform = _SYSRNG.uniform
_e = math.e
_pi = math.pi
_log = math.log


class Backoff(object):
    __slots__ = '_base', '_min_value', '_max_value', '_last_value', '_logspace', 'precomputed'

    PRECISION: ClassVar[_TypeNum] = 3
    ZERO: ClassVar[_TypeNum] = Decimal(0)
    FACTOR: ClassVar[_TypeNum] = Decimal(3)
    BASE: ClassVar[_TypeNum] = Decimal(2)
    MIN: ClassVar[_TypeNum] = Decimal(0.01)
    MAX: ClassVar[_TypeNum] = Decimal(302400.012)
    ONE_AND_HALF = Decimal(1.5)
    MAX_ATTEMPTS = 100
    JITTER_TOP_LIM = Decimal(pow(math.e, HALF_PIE) * 1.111)

    @classmethod
    def rand_factor(cls) -> float:
        return _uniform(1, _e) / _uniform(1, _pi)

    def __init__(self,

                 # optimized locals
                 log=_log,
                 MAX=MAX,
                 MIN=MIN,
                 BASE=BASE,
                 MAX_ATTEMPTS=MAX_ATTEMPTS):
        self._base = BASE
        self._min_value = MIN
        self._max_value = MAX
        self._last_value = None

        base_f = float(self._base)
        max_log_base_f = float(log(self._max_value, self._base))

        self._logspace = tuple(map(
            Decimal,
            np.logspace(
                base_f,
                max_log_base_f,
                num=MAX_ATTEMPTS,
                base=base_f)
        ))
        self.precomputed = [
            [self.value(i) for i in [it] * 33]
            for it in range(0, MAX_ATTEMPTS)
        ]

    def precomp_v(self, attempt: Union[int, float, None] = 0,
                  with_jitter: bool = False,

                  # optimized attempts
                  choice=_choice,
                  randint=_randint,
                  MAX_ATTEMPTS=MAX_ATTEMPTS) -> float:
        if attempt > MAX_ATTEMPTS:
            raise RuntimeError('Too many attempts!!!')

        if not attempt:
            attempt = randint(2, 4)

        result = choice(self.precomputed.__getitem__(attempt))

        if with_jitter:
            result *= Backoff.rand_factor()

        return self.__conform_to_range_and_type(result)

    def value(self, attempt: Union[int, float] = 0,

              # optimized locals
              e=_e,
              uniform=_uniform,
              choice=_choice,
              pow=pow,
              JITTER_TOP_LIM=JITTER_TOP_LIM,
              MAX_ATTEMPTS=MAX_ATTEMPTS) -> float:
        if attempt > MAX_ATTEMPTS:
            raise RuntimeError('Too many attempts!!!')

        if self._last_value and self._last_value >= self._max_value:
            return self.__conform_to_range_and_type(self._max_value)

        if attempt <= 0:
            return None
        else:
            result = self._logspace.__getitem__(attempt)
            if not result:
                raise ValueError(f'Attempt={attempt} not in valid range for this.')

            if result > JITTER_TOP_LIM:
                jit_top_lim = uniform(float(self._min_value), HALF_PIE)
                jitter = Decimal(pow(e, jit_top_lim))
                result = choice(_MATH_BINARY_OPS)(result, jitter)

            self._last_value = result = self.__conform_to_range_and_type(result)

            return result

    def __conform_to_range_and_type(self, result: _TypeNum = None,

                                    # optimized locals
                                    PRECISION=PRECISION,
                                    max=max,
                                    min=min,
                                    float=float,
                                    round=round) -> float:
        _result = max([
            min([
                self._max_value,
                result
            ]),
            self._min_value
        ])

        return float(round(_result, PRECISION))


__bo__ = Backoff()


def delay_with_jitter(delay: _TypeNum, min_val: _TypeNum = None):
    jitter = __bo__.precomp_v(0, with_jitter=True)

    if delay:
        delay += (1.000 / jitter)
    else:
        delay = jitter

    if min_val and delay <= min_val:
        if delay <= (min_val*0.777):
            delay += min_val
        else:
            delay += (min_val / 2.000)

    return delay
