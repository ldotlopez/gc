# -*- coding: utf-8 -*-


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
