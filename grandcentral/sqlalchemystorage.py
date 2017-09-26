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
import pickle
import time
from os import path


import sqlalchemy as sa
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

    key = sa.Column(sa.String)
    value = sa.Column(sa.String)
    timestamp = sa.Column(sa.Float, default=time.time)

    def __repr__(self):
        fmt = r"<Record(key='{key}')>"
        return fmt.format(key=self.key)


class SQLAlchemyStorage(grandcentral.storage.BaseStorage):
    def __init__(self, dbpath=None):
        if dbpath is None:
            dbpath = path.realpath(__file__)
            dbpath = path.dirname(dbpath)
            dbpath = path.dirname(dbpath)
            dbpath = dbpath + '/' + 'gc.sqlite'

        dburi = 'sqlite:///' + dbpath

        engine = sa.create_engine(dburi)
        Base.metadata.create_all(bind=engine)

        session_factory = sa.orm.sessionmaker(bind=engine)
        Session = sa.orm.scoped_session(session_factory)
        self.sess = Session()

    def read(self, key):
        qs = self.sess.query(Record)
        qs = qs.filter(Record.key == key)
        qs = qs.order_by(Record.timestamp.desc())
        record = qs.first()
        if record is None:
            raise KeyError(key)

        value = pickle.loads(record.value)

        return value

    def write(self, key, value, attachment=None):
        if attachment is not None:
            raise NotImplementedError()

        value = pickle.dumps(value)
        self.sess.add(Record(key=key, value=value))
        self.sess.commit()

    def backlog(self, key):
        qs = self.sess.query(Record)
        qs = qs.filter(Record.key == key)
        qs = qs.order_by(Record.timestamp.desc())

        # Control if at least one result was yelded instead of using qs.count() for efficience
        yielded = False
        for res in qs:
            yield pickle.loads(res.value)
            yielded = True

        if not yielded:
            raise KeyError(key)
