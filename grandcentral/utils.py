# -*- coding: utf-8 -*-


import asyncio
import functools


def resolve(coro, loop=None):
    if not asyncio.iscoroutine(coro):
        raise TypeError(coro)

    loop = loop or asyncio.get_event_loop()
    return loop.run_until_complete(coro)


def sync_coroutine(fn):
    @functools.wraps(fn)
    def _wrap(*args, **kwargs):
        return resolve(fn(*args, **kwargs))

    return _wrap
