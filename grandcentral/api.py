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


class API(falcon.API):
    def __init__(self, storage, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        doc = json.load(req.bounded_stream)

        key = doc['key']
        value = doc['value']
        self.storage.write(key, value)

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
