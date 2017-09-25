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


import unittest
import unittest.mock


import grandcentral
from grandcentral import asyncutils
# import grandcentral.sqlalchemystorage


import asyncio
import json


import falcon.testing


class TestUtils(unittest.TestCase):
    def test_resolve(self):
        async def hi_there():
            await asyncio.sleep(0.1)
            return 'hi there'

        self.assertEqual(
            asyncutils.wait_for(hi_there()),
            'hi there'
        )

    def test_sync_coroutine_decorator(self):
        @asyncutils.sync
        async def hi_there():
            await asyncio.sleep(0.1)
            return 'hi there'

        self.assertEqual(
            hi_there(),
            'hi there'
        )


class StorageTestMixin:
    def setUp(self):
        super().setUp()
        storage_cls = getattr(self, 'STORAGE_CLASS', None)
        if not storage_cls:
            raise TypeError('STORAGE_CLASS not defined')
        self.storage = storage_cls()

    def test_write(self):
        self.storage.write('x', 1)
        self.assertEqual(self.storage.read('x'), 1)

    def test_write_override(self):
        self.storage.write('x', 1)
        self.storage.write('y', 2)
        self.assertEqual(self.storage.read('y'), 2)

    def test_read_missing(self):
        with self.assertRaises(KeyError) as cm:
            self.storage.read('x')
        self.assertEqual(cm.exception.args[0], 'x')

    def test_backlog(self):
        self.storage.write('x', 1)
        self.storage.write('x', 2)
        self.assertEqual(
            list(self.storage.backlog('x')),
            [2, 1]
        )

    def test_backlog_missing(self):
        with self.assertRaises(KeyError) as cm:
            list(self.storage.backlog('x'))

        self.assertEqual(cm.exception.args[0], 'x')


class TestMemoryStorage(StorageTestMixin, unittest.TestCase):
    STORAGE_CLASS = grandcentral.storage.MemoryStorage


# class TestSQLAlchemyStorage(StorageTestMixin, unittest.TestCase):
#     class SQLAlchemyMemoryStorage(grandcentral.sqlalchemystorage.SQLAlchemyStorage):
#         def __init__(self):
#             super().__init__(dbpath=':memory:')

#     STORAGE_CLASS = SQLAlchemyMemoryStorage


class TestClient(unittest.TestCase):
    def setUp(self):
        self.client = grandcentral.Client('http://httpbin.org/')
        self.client.MESSAGE_ENDPOINT = '/anything'

    @asyncutils.sync
    async def assertWrite(self, key, value):
        res = await self.client.write(key, value)

        self.assertEqual(res['method'], 'POST')
        self.assertEqual(res['url'], 'http://httpbin.org/anything')

        expected = dict(key=key, value=value)
        self.assertEqual(
            json.loads(res['data']),
            expected)

    @asyncutils.sync
    async def test_read_message(self):
        res = await self.client.read('foo')
        self.assertEqual(res['method'], 'GET')
        self.assertEqual(res['url'], 'http://httpbin.org/anything/foo')

    def test_write_message(self):
        self.assertWrite('foo', 'bar')


class TestAPI(falcon.testing.TestCase):
    def setUp(self):
        super().setUp()
        self.storage = unittest.mock.create_autospec(grandcentral.BaseStorage)
        self.app = grandcentral.API(self.storage)

    def test_read(self):
        self.storage.read.return_value = 'bar'
        resp = self.simulate_get('/message/foo')
        self.storage.read.assert_called_with('foo')
        self.assertEqual(resp.status_code, 200)

    def test_write(self):
        self.simulate_post(
            '/message',
            headers={'content-type': falcon.MEDIA_JSON},
            body=json.dumps(
                {'key': 'foo',
                 'value': 'bar'}
            ))
        self.storage.write.assert_called_with('foo', 'bar')


if __name__ == '__main__':
    unittest.main()
