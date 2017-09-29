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


import grandcentral


import datetime
import hashlib
import os
import json
import time
from os import path


import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.ext import declarative


Base = declarative.declarative_base()
Base.metadata.naming_convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}


def now_timestamp(utc=True):
    dt = datetime.datetime.utcnow() if utc else datetime.datetime.now()
    return time.mktime(dt.timetuple())


class Record(Base):
    __tablename__ = 'records'
    __table_args__ = (
        sa.PrimaryKeyConstraint('key', 'timestamp'),
    )

    key = sa.Column(sa.String, nullable=False)
    timestamp = sa.Column(sa.Float, default=time.time)

    value = sa.Column(sa.String, nullable=False)
    attachment = sa.Column(sa.String, nullable=True, unique=False)

    def __repr__(self):
        fmt = r"<Record(key='{key}')>"
        return fmt.format(key=self.key)


class SQLAlchemyStorage(grandcentral.storage.BaseStorage):
    def __init__(self, storage_path=None):
        if storage_path is None:
            storage_path = path.realpath(__file__)
            storage_path = path.dirname(storage_path)
            storage_path = path.dirname(storage_path)
            storage_path = storage_path + '/data/'

        os.makedirs(storage_path, exist_ok=True)

        self._db_uri = 'sqlite:///' + storage_path + 'gc.sqlite'
        engine = sa.create_engine(self._db_uri)
        Base.metadata.create_all(bind=engine)

        session_factory = sa.orm.sessionmaker(bind=engine)
        Session = sa.orm.scoped_session(session_factory)
        self.sess = Session()

        self._files_path = storage_path + 'files/'
        os.makedirs(self._files_path, exist_ok=True)

    def _storage_filepath_for_attachment(self, digest):
        return '{base_path}/{digest[0]}/{digest[0]}{digest[1]}/{digest}'.format(
            base_path=self._files_path,
            digest=digest
        )

        # event.listen(Record, 'before_insert', self._on_before_insert)

    # def _on_before_insert(self, mapper, connection, target):
    #     try:
    #         prev = self.read(target.key)
    #     except KeyError:
    #         return

    #     print('{} -> {}'.format(repr(prev), repr(target.value)))

    def read(self, key):
        qs = self.sess.query(Record)
        qs = qs.filter(Record.key == key)
        qs = qs.order_by(Record.timestamp.desc())
        record = qs.first()
        if record is None:
            raise KeyError(key)

        value = json.loads(record.value)

        return value

    def write(self, key, value, attachment=None):
        record = Record(key=key, value=json.dumps(value))

        if attachment is not None:
            sha1 = hashlib.sha1()
            sha1.update(attachment)
            digest = sha1.hexdigest()

            record.attachment = digest
            storage_filepath = self._storage_filepath_for_attachment(digest)

            os.makedirs(path.dirname(storage_filepath), exist_ok=True)
            with open(storage_filepath, 'wb') as fh:
                fh.write(attachment)

        self.sess.add(record)
        self.sess.commit()

    def backlog(self, key):
        qs = self.sess.query(Record)
        qs = qs.filter(Record.key == key)
        qs = qs.order_by(Record.timestamp.desc())

        # Control if at least one result was yelded instead of using qs.count() for efficience
        yielded = False
        for res in qs:
            yield (res.timestamp, json.loads(res.value))
            yielded = True

        if not yielded:
            raise KeyError(key)
