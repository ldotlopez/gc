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


import abc


class BaseStorage:
    @abc.abstractmethod
    def read(self, key):
        raise NotImplementedError()

    @abc.abstractmethod
    def write(self, key, value):
        raise NotImplementedError()

    @abc.abstractmethod
    def backlog(self, key):
        raise NotImplementedError()


class MemoryStorage(BaseStorage):
    def __init__(self):
        self._mem = dict()

    def read(self, key):
        return self._mem[key][-1]

    def write(self, key, value):
        if key not in self._mem:
            self._mem[key] = []

        self._mem[key].append(value)

    def backlog(self, key):
        if key not in self._mem:
            raise KeyError(key)

        yield from reversed(self._mem[key])
