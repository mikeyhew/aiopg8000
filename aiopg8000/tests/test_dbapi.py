import unittest
import os
import time
import aiopg8000
import datetime
from .connection_settings import db_connect, async_test
from sys import exc_info
from aiopg8000.six import b, IS_JYTHON
from distutils.version import LooseVersion
import asyncio


# DBAPI compatible interface tests
class Tests(unittest.TestCase):
    @async_test
    def setUp(self):
        self.db = yield from aiopg8000.connect(**db_connect)
        # Jython 2.5.3 doesn't have a time.tzset() so skip
        if not IS_JYTHON:
            os.environ['TZ'] = "UTC"
            #time.tzset()

        try:
            c = yield from self.db.cursor()
            try:
                c = yield from self.db.cursor()
                yield from c.execute("DROP TABLE t1")
            except aiopg8000.DatabaseError:
                e = exc_info()[1]
                # the only acceptable error is:
                self.assertEqual(e.args[1], '42P01')  # table does not exist
                yield from self.db.rollback()
            yield from c.execute(
                "CREATE TEMPORARY TABLE t1 "
                "(f1 int primary key, f2 int not null, f3 varchar(50) null)")
            yield from c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (1, 1, None))
            yield from c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (2, 10, None))
            yield from c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (3, 100, None))
            yield from c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (4, 1000, None))
            yield from c.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (%s, %s, %s)",
                (5, 10000, None))
            yield from self.db.commit()
        finally:
            yield from c.close()

    @async_test
    def tearDown(self):
        yield from self.db.close()

    @async_test
    def testParallelQueries(self):
        try:
            c1 = yield from self.db.cursor()
            c2 = yield from self.db.cursor()

            yield from c1.execute("SELECT f1, f2, f3 FROM t1")
            while 1:
                row = yield from c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
                yield from c2.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (f1,))
                while 1:
                    row = yield from c2.fetchone()
                    if row is None:
                        break
                    f1, f2, f3 = row
        finally:
            yield from c1.close()
            yield from c2.close()

        yield from self.db.rollback()

    @async_test
    def testQmark(self):
        orig_paramstyle = aiopg8000.paramstyle
        try:
            aiopg8000.paramstyle = "qmark"
            c1 = yield from self.db.cursor()
            yield from c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > ?", (3,))
            while 1:
                row = yield from c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            yield from self.db.rollback()
        finally:
            aiopg8000.paramstyle = orig_paramstyle
            yield from c1.close()

    @async_test
    def testNumeric(self):
        orig_paramstyle = aiopg8000.paramstyle
        try:
            aiopg8000.paramstyle = "numeric"
            c1 = yield from self.db.cursor()
            yield from c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > :1", (3,))
            while 1:
                row = yield from c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            yield from self.db.rollback()
        finally:
            aiopg8000.paramstyle = orig_paramstyle
            yield from c1.close()

    @async_test
    def testNamed(self):
        orig_paramstyle = aiopg8000.paramstyle
        try:
            aiopg8000.paramstyle = "named"
            c1 = yield from self.db.cursor()
            yield from c1.execute(
                "SELECT f1, f2, f3 FROM t1 WHERE f1 > :f1", {"f1": 3})
            while 1:
                row = yield from c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            yield from self.db.rollback()
        finally:
            aiopg8000.paramstyle = orig_paramstyle
            yield from c1.close()

    @async_test
    def testFormat(self):
        orig_paramstyle = aiopg8000.paramstyle
        try:
            aiopg8000.paramstyle = "format"
            c1 = yield from self.db.cursor()
            yield from c1.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > %s", (3,))
            while 1:
                row = yield from c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            yield from self.db.commit()
        finally:
            aiopg8000.paramstyle = orig_paramstyle
            yield from c1.close()

    @async_test
    def testPyformat(self):
        orig_paramstyle = aiopg8000.paramstyle
        try:
            aiopg8000.paramstyle = "pyformat"
            c1 = yield from self.db.cursor()
            yield from c1.execute(
                "SELECT f1, f2, f3 FROM t1 WHERE f1 > %(f1)s", {"f1": 3})
            while 1:
                row = yield from c1.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row
            yield from self.db.commit()
        finally:
            aiopg8000.paramstyle = orig_paramstyle
            yield from c1.close()

    @async_test
    def testArraysize(self):
        try:
            c1 = yield from self.db.cursor()
            c1.arraysize = 3
            yield from c1.execute("SELECT * FROM t1")
            retval = yield from c1.fetchmany()
            self.assertEqual(len(retval), c1.arraysize)
        finally:
            yield from c1.close()
        yield from self.db.commit()

    def testDate(self):
        val = aiopg8000.Date(2001, 2, 3)
        self.assertEqual(val, datetime.date(2001, 2, 3))

    def testTime(self):
        val = aiopg8000.Time(4, 5, 6)
        self.assertEqual(val, datetime.time(4, 5, 6))

    def testTimestamp(self):
        val = aiopg8000.Timestamp(2001, 2, 3, 4, 5, 6)
        self.assertEqual(val, datetime.datetime(2001, 2, 3, 4, 5, 6))

    def testDateFromTicks(self):
        if IS_JYTHON:
            return

        val = aiopg8000.DateFromTicks(1173804319)
        self.assertEqual(val, datetime.date(2007, 3, 13))

    def testTimeFromTicks(self):
        if IS_JYTHON:
            return

        val = aiopg8000.TimeFromTicks(1173804319)
        self.assertEqual(val, datetime.time(16, 45, 19))

    def testTimestampFromTicks(self):
        if IS_JYTHON:
            return

        val = aiopg8000.TimestampFromTicks(1173804319)
        self.assertEqual(val, datetime.datetime(2007, 3, 13, 16, 45, 19))

    def testBinary(self):
        v = aiopg8000.Binary(b("\x00\x01\x02\x03\x02\x01\x00"))
        self.assertEqual(v, b("\x00\x01\x02\x03\x02\x01\x00"))
        self.assertTrue(isinstance(v, aiopg8000.BINARY))

    @async_test
    def testRowCount(self):
        try:
            c1 = yield from self.db.cursor()
            yield from c1.execute("SELECT * FROM t1")

            # Before PostgreSQL 9 we don't know the row count for a select
            if self.db._server_version > LooseVersion('8.0.0'):
                self.assertEqual(5, c1.rowcount)

            yield from c1.execute("UPDATE t1 SET f3 = %s WHERE f2 > 101", ("Hello!",))
            self.assertEqual(2, c1.rowcount)

            yield from c1.execute("DELETE FROM t1")
            self.assertEqual(5, c1.rowcount)
        finally:
            yield from c1.close()
        yield from self.db.commit()

    @async_test
    def testFetchMany(self):
        try:
            cursor = yield from self.db.cursor()
            cursor.arraysize = 2
            yield from cursor.execute("SELECT * FROM t1")
            self.assertEqual(2, len((yield from cursor.fetchmany())))
            self.assertEqual(2, len((yield from cursor.fetchmany())))
            self.assertEqual(1, len((yield from cursor.fetchmany())))
            self.assertEqual(0, len((yield from cursor.fetchmany())))
        finally:
            yield from cursor.close()
        yield from self.db.commit()

    @async_test
    def testIterator(self):
        from warnings import filterwarnings
        filterwarnings("ignore", "DB-API extension cursor.next()")
        filterwarnings("ignore", "DB-API extension cursor.__iter__()")

        try:
            cursor = yield from self.db.cursor()
            yield from cursor.execute("SELECT * FROM t1 ORDER BY f1")
            f1 = 0
            for row in cursor:
                next_f1 = row[0]
                assert next_f1 > f1
                f1 = next_f1
        except:
            yield from cursor.close()

        yield from self.db.commit()

    # Vacuum can't be run inside a transaction, so we need to turn
    # autocommit on.
    @async_test
    def testVacuum(self):
        self.db.autocommit = True
        try:
            cursor = yield from self.db.cursor()
            yield from cursor.execute("vacuum")
        finally:
            yield from cursor.close()

    # If autocommit is on and we do an operation that returns more rows than
    # the cache holds, make sure exception raised.
    
    def testAutocommitMaxRows(self):

        @async_test
        def test_wrapper():
            self.db.autocommit = True
            try:
                cursor = yield from  self.db.cursor()

                yield from cursor.execute("select generate_series(1, " +
                                str(aiopg8000.core.Connection._row_cache_size + 1) + ")")

            finally:
                yield from cursor.close()

        self.assertRaises(aiopg8000.InterfaceError, test_wrapper)
if __name__ == "__main__":
    unittest.main()
