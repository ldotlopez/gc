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


import grandcentral.storage


import json


import falcon
import falcon_multipart.middleware
import mimeparse


class API(falcon.API):
    def __init__(self, storage, *args, **kwargs):
        super().__init__(
            *args,
            middleware=[falcon_multipart.middleware.MultipartMiddleware()],
            **kwargs)

        if not isinstance(storage, grandcentral.storage.BaseStorage):
            raise TypeError(storage)

        self.storage = storage
        self.signaler = None

        msg_col_rsrc = MessagesCollectionResource(storage=self.storage)
        msg_item_rsrc = MessagesItemResource(storage=self.storage)
        self.add_route('/message', msg_col_rsrc)
        self.add_route('/message/{key}', msg_item_rsrc)


class MessagesCollectionResource:
    def __init__(self, storage):
        self.storage = storage

    def on_post(self, req, resp):
        typ, subtyp, props = mimeparse.parse_mime_type(req.content_type)

        if typ == 'multipart' and subtyp in ['form-data', 'mixed']:
            # Handle multipart uploads

            message = req.get_param('message')

            # Message not found in request
            if not message:
                raise ValueError('Missing message')

            # Invalid json for message
            try:
                message = json.loads(message)
            except json.decoder.JSONDecodeError as e:
                raise ValueError('Malformed message') from e

            attachment = req.get_param('attachment')

        elif typ == 'application' and subtyp == 'json':
            # Handle simple requests

            # Invalid json for message
            try:
                message = json.load(req.stream)
            except json.decoder.JSONDecodeError as e:
                raise ValueError('Malformed message') from e

            attachment = None

        else:
            raise ValueError('Unknow request')

        try:
            key = message['key']
            value = message['value']
        except KeyError as e:
            raise ValueError('Malformed message') from e

        self.storage.write(key, value, attachment)

        resp.status = falcon.HTTP_204


class MessagesItemResource:
    def __init__(self, storage):
        self.storage = storage

    def on_get(self, req, resp, key):
        try:
            value = self.storage.read(key)

        except KeyError:
            resp.status = falcon.HTTP_404
            resp.body = json.dumps({
                'key': key
            })
            return

        resp.status = falcon.HTTP_200
        resp.content_type = falcon.MEDIA_JSON
        resp.body = json.dumps({
            'key': key,
            'value': value
        })
