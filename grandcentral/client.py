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


import io
import json
import pathlib


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

    async def backlog(self, key):
        resp = await self._request(
            'GET',
            self.MESSAGE_ENDPOINT + '/{}/backlog'.format(key))

        if resp.status == 200:
            return await resp.json()

        elif resp.status == 404:
            raise KeyError(key)

        else:
            raise TypeError()

    async def write(self, key, value, attachment=None):
        req_kwargs = self._build_request_arguments(key, value, attachment)
        resp = await self._request('POST', self.MESSAGE_ENDPOINT, **req_kwargs)

        if resp.status == 204:
            resp.close()
            return

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

    async def _request(self, method, path, *args, **kwargs):
        url = self.api + path
        async with aiohttp.ClientSession() as sess:
            async with sess.request(method, url, *args, **kwargs) as resp:
                return resp

    def _build_request_arguments(self, key, value, attachment=None):
        json_data = {
            'key': key,
            'value': value
        }

        if attachment is None:
            return dict(json=json_data)

        elif isinstance(attachment, pathlib.Path):
            with attachment.open('rb') as fh:
                attachment = fh.read()

        elif isinstance(attachment, (io.BufferedIOBase, io.TextIOBase)):
            attachment = attachment.read()

        with aiohttp.MultipartWriter('form-data') as payload:
            payload.append(json.dumps(json_data), {
                'Content-Type': 'application/json',
                'Content-Disposition': 'form-data; name="message"'
            })
            payload.append(attachment, {
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': 'form-data; name="attachment"; filename="uploaded-file"'
            })

        return dict(data=payload)


class SyncClient(Client):
    @asyncutils.sync
    async def read(self, key):
        return (await super().read(key))

    @asyncutils.sync
    async def backlog(self, key):
        return (await super().backlog(key))

    @asyncutils.sync
    async def write(self, key, value, attachment=None):
        return (await super().write(key, value, attachment))

    @asyncutils.sync
    async def query(self, key):
        return (await super().query(key))


def main():
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-u', '--url',
        default='http://localhost:8000/',
        help='Grand Central destination server')
    parser.add_argument(
        '--json',
        default=False,
        action='store_true',
        help='Interpret supplied value as a json')
    parser.add_argument(
        '--attachment',
        help='Attach file to key')
    parser.add_argument(
        '--backlog',
        action='store_true',
        help='Show backlog for a key')
    parser.add_argument(
        dest='key',
        help='Destination key')
    parser.add_argument(
        dest='value',
        nargs='?',
        help='Value to store')

    args = parser.parse_args(sys.argv[1:])

    if args.backlog and (args.value or args.attachment):
        msg = "backlog and value or attachments is not a valid operation"
        raise ValueError(msg)

    if args.attachment and not args.value:
        msg = "attachment needs a value"
        raise ValueError(msg)

    client = SyncClient(args.url)

    # Mode: backlog
    if args.backlog:
        for x in client.backlog(args.key):
            value = x['message']['value']
            if args.json:
                value = json.loads(value)

            print('{timestamp}: {value}'.format(
                timestamp=x['timestamp'],
                value=value
                ))

    # Mode: read
    elif not args.value:
        value = client.read(args.key)
        print(repr(value) if args.json else value)

    # Mode: write
    else:
        if args.json:
            args.value = json.loads(args.value)

        if args.attachment:
            args.attachment = pathlib.Path(args.attachment)

        client.write(
            args.key,
            args.value,
            args.attachment)


if __name__ == '__main__':
    main()
