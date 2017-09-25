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


from grandcentral import asyncutils


import json


import aiohttp


class Client:
    MESSAGE_ENDPOINT = '/message'

    def __init__(self, api_url):
        self.api = api_url.rstrip('/')

    async def read(self, key):
        resp = await self._request(
            'GET',
            self.MESSAGE_ENDPOINT + '/{}'.format(key))

        if resp.status == 200:
            doc = (await resp.json())
            return doc['value']

        elif resp.status == 404:
            raise KeyError(key)

        else:
            raise TypeError()

    async def history(self, key):
        resp = await self._request(
            'GET',
            self.MESSAGE_ENDPOINT + '/{}'.format(key))

        if resp.status == 200:
            doc = (await resp.json())
            return doc['values']

        elif resp.status == 404:
            raise KeyError(key)

        else:
            raise TypeError()

    async def query(self, key, min=None, max=None):
        resp = await self._request(
            'GET',
            self.MESSAGE_ENDPOINT,
            query_string='key={}'.format(key))

        return (await resp.json())

    async def write(self, key, value):
        payload = json.dumps({'key': key, 'value': value})
        resp = await self._request(
            'POST',
            self.MESSAGE_ENDPOINT,
            data=payload)

        if resp.status == 204:
            resp.close()
            return

        elif resp.status == 404:
            raise KeyError(key)

        else:
            raise TypeError()

    async def _request(self, method, path, *args, **kwargs):
        url = self.api + path
        async with aiohttp.ClientSession() as sess:
            async with sess.request(method, url, *args, **kwargs) as resp:
                return resp


class SyncClient(Client):
    @asyncutils.sync
    async def read(self, key):
        return (await super().read(key))

    @asyncutils.sync
    async def write(self, key, value):
        return (await super().write(key, value))

    @asyncutils.sync
    async def query(self, key):
        return (await super().query(key))


def main():
    import argparse
    import sys
    from grandcentral.utils import resolve as r

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', default='http://localhost:8000/')
    parser.add_argument('--json', action='store_true')
    parser.add_argument(dest='args', nargs='+')

    args = parser.parse_args(sys.argv[1:])
    n_args = len(args.args)

    if n_args == 1:
        client = Client(args.url)

        key, = args.args
        value = r(client.read(key))

        if args.json:
            print(repr(value))
        else:
            print(value)

    elif n_args == 2:
        client = Client(args.url)

        key, value = args.args
        if args.json:
            value = json.loads(value)

        r(client.write(key, value))

    else:
        raise TypeError()


if __name__ == '__main__':
    main()
