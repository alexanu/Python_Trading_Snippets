# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from settings import MYSQL_CONF


class SQLManager(object):

    def __init__(self):
        db_url = 'mysql://{USERNAME}:{PASSWORD}@{HOST}:' \
                    '{PORT}/{DB}?charset={CHARSET}'\
                    .format(**MYSQL_CONF)
        self.engine = create_engine(db_url, echo=False)
        self.session = Session(self.engine)
        self.conn = self.engine.connect()

    def __del__(self):
        self.conn.close()

    def execute(self, sql):
        if sql.lower().startswith('select'):
            return self.conn.execute(text(sql))
        try:
            self.conn.execute(text(sql))
            self.session.commit()
            return True
        except SQLAlchemyError:
            self.session.rollback()
        return False
