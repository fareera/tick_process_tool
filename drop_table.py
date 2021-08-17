from peewee import PostgresqlDatabase

from utils.common_utils import create_peewee_table_class

DBCONN = PostgresqlDatabase('candleline', **{
    'host': '114.96.104.203',
    'password': 'Biz@2021@#',
    'port': 5432,
    'user': 'admin'
})

tables = DBCONN.get_tables()
DBCONN.drop_tables([create_peewee_table_class("_", "_", table_name=i) for i in tables])
