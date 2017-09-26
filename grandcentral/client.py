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
            params='key={}'.format(key))

        return (await resp.json())

    async def write(self, key, value, attachment=None):
        json_data = {'key': key, 'value': value}

        if attachment is not None:
            with aiohttp.MultipartWriter('form-data') as payload:
                payload.append(json.dumps(json_data), {
                    'CONTENT-TYPE': 'application/json',
                    'Content-Disposition': 'form-data; name="message"'
                })
                payload.append(attachment, {
                    'CONTENT-TYPE': 'application/octet-stream',
                    'Content-Disposition': 'form-data; name="attachment"'
                })

            req_params = {'data': payload}
        else:
            req_params = {'json': json_data}

        resp = await self._request('POST', self.MESSAGE_ENDPOINT, **req_params)

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
    async def write(self, key, value, attachment=None):
        return (await super().write(key, value, attachment))

    @asyncutils.sync
    async def query(self, key):
        return (await super().query(key))


def main():
    import argparse
    import sys
    from grandcentral.asyncutils import wait_for
    _undef = object()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-u', '--url',
        default='http://localhost:8000/',
        help='Grand Central destination server'
    )
    parser.add_argument(
        '--json',
        default=False,
        action='store_true',
        help='Interpret supplied value as a json'
    )
    parser.add_argument(
        '-a',
        '--attachment',
        help='Attach file to key'
    )
    parser.add_argument(
        dest='key',
        help='Destination key'
        )
    parser.add_argument(
        dest='value',
        nargs='?',
        default=_undef,
        help='Value to store'
    )

    args = parser.parse_args(sys.argv[1:])

    if args.value is _undef:
        client = Client(args.url)

        value = wait_for(client.read(args.key))

        if args.json:
            print(repr(value))
        else:
            print(value)

    else:
        client = Client(args.url)

        value = args.value
        if args.json:
            value = json.loads(value)

        if args.attachment:
            with open(args.attachment, 'rb') as fh:
                attachment = fh.read()
        else:
            attachment = None

        wait_for(client.write(args.key, value, attachment))


if __name__ == '__main__':
    main()
