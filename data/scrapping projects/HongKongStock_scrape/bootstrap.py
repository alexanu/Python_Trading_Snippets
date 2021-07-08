# -*- coding: utf-8 -*-

from sqlalchemy import Table, Column, Integer,\
    String, Float, DateTime, create_engine, \
    MetaData, UniqueConstraint
from sqlalchemy.sql import text
from HongKongStock.settings import MYSQL_CONF


meta = MetaData()

SpecificStock = Table('SpecificStock', meta,
                      Column('id', Integer, primary_key=True),
                      Column('stock_id', String(20), index=True),
                      Column('create_date', DateTime, index=True),
                      Column('status', String(20)),
                      Column('BrokerNo', String(30)),
                      Column('turnover', String(20)),
                      Column('shares', String(20)),
                      Column('percent', Float),
                      Column('AV', String(20)),
                      # UniqueConstraint('stock_id', 'create_date')
                      )

ShortSelling = Table('ShortSelling', meta,
                     Column('id', Integer, primary_key=True),
                     Column('stock_id', String(20), index=True),
                     Column('BrokerNo', String(30)),
                     Column('create_date', DateTime, index=True),
                     Column('short_selling_amount', String(20)),
                     Column('short_selling_amount_extent', Float),
                     Column('short_selling_amount_accounting', Float),
                     Column('short_selling_turnover', String(20)),
                     Column('short_selling_turnover_extent', Float),
                     Column('short_selling_turnover_accounting', Float),
                     Column('price', Float),
                     Column('increase', Float),
                     Column('increase_amount', Float),
                     Column('volumn', String(20)),
                     Column('turnover', String(20)),
                     # UniqueConstraint('stock_id', 'create_date')
                     )

CashStk = Table('CashStk', meta,
                Column('id', Integer, primary_key=True),
                Column('stock_id', String(20), index=True),
                Column('BrokerNo', String(30)),
                Column('create_date', DateTime, index=True),
                Column('increase', String(20)),
                Column('price', Float),
                Column('inflow_funds', String(20)),
                Column('outflow_funds', String(20)),
                Column('net_inflow', String(20)),
                Column('net_inflow_rate', String(20)),
                Column('small_net_inflow',String(20)),
                Column('small_accounting', String(20)),
                Column('medium_net_inflow', String(20)),
                Column('medium_net_accounting', String(20)),
                Column('big_net_inflow', String(20)),
                Column('big_accounting', String(20)),
                Column('large_net_inflow', String(20)),
                Column('large_accounting', String(20)),
                # UniqueConstraint('stock_id', 'create_date')
                )


mysql_url = 'mysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}'.\
                format(**MYSQL_CONF)
db_url = 'mysql://{USERNAME}:{PASSWORD}@{HOST}:' \
                    '{PORT}/{DB}?charset={CHARSET}'\
                    .format(**MYSQL_CONF)

engine = create_engine(mysql_url, echo=False)
conn = engine.connect()
conn.execute(text('CREATE DATABASE IF NOT EXISTS {DB} '
                  'default character set {CHARSET}'.
                           format(**MYSQL_CONF)))
engine = create_engine(db_url, echo=False)
meta.drop_all(engine)
meta.create_all(engine)
