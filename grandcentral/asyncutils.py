# -*- coding: utf-8 -*-

# Copyright (C) 2017 Luis López <luis@cuarentaydos.com>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
# USA.


import asyncio
import functools


def wait_for(coro, loop=None):
    if not asyncio.iscoroutine(coro):
        raise TypeError(coro)

    loop = loop or asyncio.get_event_loop()
    return loop.run_until_complete(coro)


def sync(fn):
    @functools.wraps(fn)
    def _wrap(*args, **kwargs):
        return wait_for(fn(*args, **kwargs))

    return _wrap
