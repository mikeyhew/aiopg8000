import os
import asyncio
import copy

'''
db_stewart_connect = {
    "host": "127.0.0.1",
    "user": "pg8000-test",
    "database": "pg8000-test",
    "password": "pg8000-test",
    "socket_timeout": 5,
    "ssl": False}

db_local_connect = {
    "unix_sock": "/tmp/.s.PGSQL.5432",
    "user": "mfenniak"}

db_local_win_connect = {
    "host": "localhost",
    "user": "mfenniak",
    "password": "password",
    "database": "mfenniak"}

db_oracledev2_connect = {
    "host": "oracledev2",
    "user": "mfenniak",
    "password": "password",
    "database": "mfenniak"}
'''

NAME_VAR = "PG8000_TEST_NAME"
try:
    TEST_NAME = os.environ[NAME_VAR]
except KeyError:
    raise Exception(
        "The environment variable " + NAME_VAR + " needs to be set. It should "
        "contain the name of the environment variable that contains the "
        "kwargs for the connect() function.")

db_connect0 = eval(os.environ[TEST_NAME])


#from http://stackoverflow.com/a/23036785/586784
def async_test(f):
    def wrapper(*args, **kwargs):
        coro = asyncio.coroutine(f)
        future = coro(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper

def stream_generator():
    return (yield from asyncio.open_connection(host=db_connect0['host'], port=db_connect0['port'], ssl=db_connect0['ssl']))

db_connect = dict(user=db_connect0['user'], password=db_connect0['password'], database=db_connect0['database'], stream_generator=stream_generator)
