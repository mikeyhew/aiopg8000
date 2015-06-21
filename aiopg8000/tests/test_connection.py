import unittest
import pg8000
from pg8000.tests.connection_settings import db_connect, async_test
from pg8000.six import PY2, PRE_26
import asyncio

# Tests related to connecting to a database.
class Tests(unittest.TestCase):
    def testSocketMissing(self):
        conn_params = {
            'unix_sock': "/file-does-not-exist",
            'user': "doesn't-matter"}
        if 'use_cache' in db_connect:
            conn_params['use_cache'] = db_connect['use_cache']
        self.assertRaises(pg8000.InterfaceError, pg8000.connect, **conn_params)

    def testDatabaseMissing(self):
        data = db_connect.copy()
        data["database"] = "missing-db"
        self.assertRaises(pg8000.ProgrammingError, pg8000.connect, **data)

    @async_test
    def testNotify(self):

        try:
            db = yield from pg8000.connect(**db_connect)
            self.assertEqual(db.notifies, [])
            cursor = yield from  db.cursor()
            yield from cursor.execute("LISTEN test")
            yield from cursor.execute("NOTIFY test")
            yield from db.commit()

            yield from cursor.execute("VALUES (1, 2), (3, 4), (5, 6)")
            self.assertEqual(len(db.notifies), 1)
            self.assertEqual(db.notifies[0][1], "test")
        finally:
            yield from cursor.close()
            yield from db.close()

    # This requires a line in pg_hba.conf that requires md5 for the database
    # pg8000_md5

    def testMd5(self):
        data = db_connect.copy()
        data["database"] = "pg8000_md5"

        # Should only raise an exception saying db doesn't exist
        if PY2:
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

    # This requires a line in pg_hba.conf that requires gss for the database
    # pg8000_gss

    def testGss(self):
        data = db_connect.copy()
        data["database"] = "pg8000_gss"

        # Should raise an exception saying gss isn't supported
        if PY2:
            self.assertRaises(pg8000.InterfaceError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.InterfaceError,
                "Authentication method 7 not supported by pg8000.",
                pg8000.connect, **data)

    def testSsl(self):
        data = db_connect.copy()
        data["ssl"] = True
        if PRE_26:
            self.assertRaises(pg8000.InterfaceError, pg8000.connect, **data)
        else:
            db = pg8000.connect(**data)
            db.close()

    # This requires a line in pg_hba.conf that requires 'password' for the
    # database pg8000_password

    def testPassword(self):
        data = db_connect.copy()
        data["database"] = "pg8000_password"

        # Should only raise an exception saying db doesn't exist
        if PY2:
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

    def testUnicodeDatabaseName(self):
        data = db_connect.copy()
        data["database"] = "pg8000_sn\uFF6Fw"

        # Should only raise an exception saying db doesn't exist
        if PY2:
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

    def testBytesDatabaseName(self):
        data = db_connect.copy()

        # Should only raise an exception saying db doesn't exist
        if PY2:
            data["database"] = "pg8000_sn\uFF6Fw"
            self.assertRaises(
                pg8000.ProgrammingError, pg8000.connect, **data)
        else:
            data["database"] = bytes("pg8000_sn\uFF6Fw", 'utf8')
            self.assertRaisesRegex(
                pg8000.ProgrammingError, '3D000', pg8000.connect, **data)

    def testBrokenPipe(self):
        @async_test
        def wrapper():
            d1 = None
            d2 = None
            try:
                db1 = yield from pg8000.connect(**db_connect)
                db2 = yield from pg8000.connect(**db_connect)

                cur1 = yield from db1.cursor()
                cur2 = yield from db2.cursor()

                yield from cur1.execute("select pg_backend_pid()")
                pid1 = (yield from cur1.fetchone())[0]


                yield from cur2.execute("select pg_terminate_backend(%s)", (pid1,))

                #should throw here
                yield from cur1.execute("select 1")

                yield from d1.close()
            finally:
                yield from cur2.close()
                yield from db2.close()

        self.assertRaises(pg8000.OperationalError, wrapper)


if __name__ == "__main__":
    unittest.main()
