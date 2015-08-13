import unittest
import aiopg8000
import datetime
import decimal
import struct
from .connection_settings import db_connect, async_test
from aiopg8000.six import b, IS_JYTHON, text_type, PY2
import uuid
import os
import time
from distutils.version import LooseVersion
import sys


if not IS_JYTHON:
    import pytz


# Type conversion tests
class PreparedTests(unittest.TestCase):
    @async_test
    def setUp(self):
        self.db = yield from aiopg8000.connect(**db_connect)
        self.cursor = yield from self.db.cursor()
        self.db.prepared = True

    @async_test
    def tearDown(self):
        self.cursor.close()
        self.cursor = None
        self.db.close()

    @async_test
    def testTimeRoundtrip(self):
        yield from self.cursor.execute("SELECT %s as f1", (datetime.time(4, 5, 6),))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], datetime.time(4, 5, 6))

    @async_test
    def testDateRoundtrip(self):
        yield from self.cursor.execute("SELECT %s as f1", (datetime.date(2001, 2, 3),))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], datetime.date(2001, 2, 3))

    @async_test
    def testBoolRoundtrip(self):
        yield from self.cursor.execute("SELECT %s as f1", (True,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], True)

    @async_test
    def testNullRoundtrip(self):
        # We can't just "SELECT %s" and set None as the parameter, since it has
        # no type.  That would result in a PG error, "could not determine data
        # type of parameter %s".  So we create a temporary table, insert null
        # values, and read them back.
        yield from self.cursor.execute(
            "CREATE TEMPORARY TABLE TestNullWrite "
            "(f1 int4, f2 timestamp, f3 varchar)")
        yield from self.cursor.execute(
            "INSERT INTO TestNullWrite VALUES (%s, %s, %s)",
            (None, None, None,))
        yield from self.cursor.execute("SELECT * FROM TestNullWrite")
        retval = yield from self.cursor.fetchone()
        self.assertEqual(retval, (None, None, None))


    def testNullSelectFailure(self):
        # See comment in TestNullRoundtrip.  This test is here to ensure that
        # this behaviour is documented and doesn't mysteriously change.

        @async_test
        def do_it():
            try:
                yield from self.cursor.execute("SELECT %s as f1", (None,))
            finally:
                yield from self.db.rollback()
        self.assertRaises(
            aiopg8000.ProgrammingError, do_it)

    @async_test
    def testDecimalRoundtrip(self):
        values = (
            "1.1", "-1.1", "10000", "20000", "-1000000000.123456789", "1.0",
            "12.44")
        for v in values:
            yield from self.cursor.execute("SELECT %s as f1", (decimal.Decimal(v),))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(str(retval[0][0]), v)

    @async_test
    def testFloatRoundtrip(self):
        # This test ensures that the binary float value doesn't change in a
        # roundtrip to the server.  That could happen if the value was
        # converted to text and got rounded by a decimal place somewhere.
        val = 1.756e-12
        bin_orig = struct.pack("!d", val)
        yield from self.cursor.execute("SELECT %s as f1", (val,))
        retval = yield from self.cursor.fetchall()
        bin_new = struct.pack("!d", retval[0][0])
        self.assertEqual(bin_new, bin_orig)

    @async_test
    def testStrRoundtrip(self):
        v = "hello world"
        yield from self.cursor.execute(
            "create temporary table test_str (f character varying(255))")
        yield from self.cursor.execute("INSERT INTO test_str VALUES (%s)", (v,))
        yield from self.cursor.execute("SELECT * from test_str")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

    @async_test
    def testUnicodeRoundtrip(self):
        yield from self.cursor.execute(
            "SELECT cast(%s as varchar) as f1", ("hello \u0173 world",))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], "hello \u0173 world")

        v = text_type("hello \u0173 world")
        yield from self.cursor.execute("SELECT cast(%s as varchar) as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

    @async_test
    def testLongRoundtrip(self):
        yield from self.cursor.execute(
            "SELECT cast(%s as bigint)", (50000000000000,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], 50000000000000)

    @async_test
    def testIntExecuteMany(self):
        yield from self.cursor.executemany("SELECT cast(%s as integer)", ((1,), (40000,)))
        yield from self.cursor.fetchall()

        v = ((None,), (4,))
        yield from self.cursor.execute(
            "create temporary table test_int (f integer)")
        yield from self.cursor.executemany("INSERT INTO test_int VALUES (%s)", v)
        yield from self.cursor.execute("SELECT * from test_int")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval, v)

    @async_test
    def testIntRoundtrip(self):
        int2 = 21
        int4 = 23
        int8 = 20

        test_values = [
            (0, int2, 'smallint'),
            (-32767, int2, 'smallint'),
            (-32768, int4, 'integer'),
            (+32767, int2, 'smallint'),
            (+32768, int4, 'integer'),
            (-2147483647, int4, 'integer'),
            (-2147483648, int8, 'bigint'),
            (+2147483647, int4, 'integer'),
            (+2147483648, int8, 'bigint'),
            (-9223372036854775807, int8, 'bigint'),
            (+9223372036854775807, int8, 'bigint'), ]

        for value, typoid, tp in test_values:
            yield from self.cursor.execute("SELECT cast(%s as " + tp + ")", (value,))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(retval[0][0], value)
            column_name, column_typeoid = self.cursor.description[0][0:2]
            self.assertEqual(column_typeoid, typoid)

    @async_test
    def testByteaRoundtrip(self):
        yield from self.cursor.execute(
            "SELECT %s as f1",
            (aiopg8000.Binary(b("\x00\x01\x02\x03\x02\x01\x00")),))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], b("\x00\x01\x02\x03\x02\x01\x00"))

    @async_test
    def testTimestampRoundtrip(self):
        v = datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)
        yield from self.cursor.execute("SELECT %s as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

        # Test that time zone doesn't affect it
        # Jython 2.5.3 doesn't have a time.tzset() so skip
        if not IS_JYTHON:
            orig_tz = os.environ['TZ']
            os.environ['TZ'] = "America/Edmonton"
            time.tzset()

            yield from self.cursor.execute("SELECT %s as f1", (v,))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(retval[0][0], v)

            os.environ['TZ'] = orig_tz
            time.tzset()

    @async_test
    def testIntervalRoundtrip(self):
        v = aiopg8000.Interval(microseconds=123456789, days=2, months=24)
        yield from self.cursor.execute("SELECT %s as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

        v = datetime.timedelta(seconds=30)
        yield from self.cursor.execute("SELECT %s as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

    @async_test
    def testEnumRoundtrip(self):
        try:
            yield from self.cursor.execute(
                "create type lepton as enum ('electron', 'muon', 'tau')")
        except aiopg8000.ProgrammingError:
            yield from self.db.rollback()

        v = 'muon'
        yield from self.cursor.execute("SELECT cast(%s as lepton) as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)
        yield from self.cursor.execute(
            "CREATE TEMPORARY TABLE testenum "
            "(f1 lepton)")
        yield from self.cursor.execute("INSERT INTO testenum VALUES (%s)", ('electron',))
        yield from self.cursor.execute("drop table testenum")
        yield from self.cursor.execute("drop type lepton")
        yield from self.db.commit()

    @async_test
    def testXmlRoundtrip(self):
        v = '<genome>gatccgagtac</genome>'
        yield from self.cursor.execute("select xmlparse(content %s) as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

    @async_test
    def testUuidRoundtrip(self):
        v = uuid.UUID('911460f2-1f43-fea2-3e2c-e01fd5b5069d')
        yield from self.cursor.execute("select %s as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

    @async_test
    def testInetRoundtrip(self):
        try:
            import ipaddress

            v = ipaddress.ip_network('192.168.0.0/28')
            yield from self.cursor.execute("select %s as f1", (v,))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(retval[0][0], v)

            v = ipaddress.ip_address('192.168.0.1')
            yield from self.cursor.execute("select %s as f1", (v,))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(retval[0][0], v)

        except ImportError:
            for v in ('192.168.100.128/25', '192.168.0.1'):
                yield from self.cursor.execute(
                    "select cast(cast(%s as varchar) as inet) as f1", (v,))
                retval = yield from self.cursor.fetchall()
                self.assertEqual(retval[0][0], v)

    @async_test
    def testXidRoundtrip(self):
        v = 86722
        yield from self.cursor.execute(
            "select cast(cast(%s as varchar) as xid) as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

        # Should complete without an exception
        yield from self.cursor.execute(
            "select * from pg_locks where transactionid = %s", (97712,))
        retval = yield from self.cursor.fetchall()

    @async_test
    def testInt2VectorIn(self):
        yield from self.cursor.execute("select cast('1 2' as int2vector) as f1")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], [1, 2])

        # Should complete without an exception
        yield from self.cursor.execute("select indkey from pg_index")
        retval = yield from self.cursor.fetchall()

    @async_test
    def testTimestampTzOut(self):
        yield from self.cursor.execute(
            "SELECT '2001-02-03 04:05:06.17 America/Edmonton'"
            "::timestamp with time zone")
        retval = yield from self.cursor.fetchall()
        dt = retval[0][0]
        self.assertEqual(dt.tzinfo is not None, True, "no tzinfo returned")
        self.assertEqual(
            dt.astimezone(aiopg8000.utc),
            datetime.datetime(2001, 2, 3, 11, 5, 6, 170000, aiopg8000.utc),
            "retrieved value match failed")

    @async_test
    def testTimestampTzRoundtrip(self):
        if not IS_JYTHON:
            mst = pytz.timezone("America/Edmonton")
            v1 = mst.localize(datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))
            yield from self.cursor.execute("SELECT %s as f1", (v1,))
            retval = yield from self.cursor.fetchall()
            v2 = retval[0][0]
            self.assertNotEqual(v2.tzinfo, None)
            self.assertEqual(v1, v2)

    @async_test
    def testTimestampMismatch(self):
        if not IS_JYTHON:
            mst = pytz.timezone("America/Edmonton")
            yield from self.cursor.execute("SET SESSION TIME ZONE 'America/Edmonton'")
            try:
                yield from self.cursor.execute(
                    "CREATE TEMPORARY TABLE TestTz "
                    "(f1 timestamp with time zone, "
                    "f2 timestamp without time zone)")
                yield from self.cursor.execute(
                    "INSERT INTO TestTz (f1, f2) VALUES (%s, %s)", (
                        # insert timestamp into timestamptz field (v1)
                        datetime.datetime(2001, 2, 3, 4, 5, 6, 170000),
                        # insert timestamptz into timestamp field (v2)
                        mst.localize(datetime.datetime(
                            2001, 2, 3, 4, 5, 6, 170000))))
                yield from self.cursor.execute("SELECT f1, f2 FROM TestTz")
                retval = yield from self.cursor.fetchall()

                # when inserting a timestamp into a timestamptz field,
                # postgresql assumes that it is in local time. So the value
                # that comes out will be the server's local time interpretation
                # of v1. We've set the server's TZ to MST, the time should
                # be...
                f1 = retval[0][0]
                self.assertEqual(
                    f1, datetime.datetime(
                        2001, 2, 3, 11, 5, 6, 170000, pytz.utc))

                # inserting the timestamptz into a timestamp field, aiopg8000
                # converts the value into UTC, and then the PG server converts
                # it into local time for insertion into the field. When we
                # query for it, we get the same time back, like the tz was
                # dropped.
                f2 = retval[0][1]
                self.assertEqual(
                    f2, datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))
            finally:
                yield from self.cursor.execute("SET SESSION TIME ZONE DEFAULT")

    @async_test
    def testNameOut(self):
        # select a field that is of "name" type:
        yield from self.cursor.execute("SELECT usename FROM pg_user")
        self.cursor.fetchall()
        # It is sufficient that no errors were encountered.

    @async_test
    def testOidOut(self):
        yield from self.cursor.execute("SELECT oid FROM pg_type")
        self.cursor.fetchall()
        # It is sufficient that no errors were encountered.

    @async_test
    def testBooleanOut(self):
        yield from self.cursor.execute("SELECT cast('t' as bool)")
        retval = yield from self.cursor.fetchall()
        self.assertTrue(retval[0][0])

    @async_test
    def testNumericOut(self):
        for num in ('5000', '50.34'):
            yield from self.cursor.execute("SELECT " + num + "::numeric")
            retval = yield from self.cursor.fetchall()
            self.assertEqual(str(retval[0][0]), num)

    @async_test
    def testInt2Out(self):
        yield from self.cursor.execute("SELECT 5000::smallint")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], 5000)

    @async_test
    def testInt4Out(self):
        yield from self.cursor.execute("SELECT 5000::integer")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], 5000)

    @async_test
    def testInt8Out(self):
        yield from self.cursor.execute("SELECT 50000000000000::bigint")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], 50000000000000)

    @async_test
    def testFloat4Out(self):
        yield from self.cursor.execute("SELECT 1.1::real")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], 1.1000000238418579)

    @async_test
    def testFloat8Out(self):
        yield from self.cursor.execute("SELECT 1.1::double precision")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], 1.1000000000000001)

    @async_test
    def testVarcharOut(self):
        yield from self.cursor.execute("SELECT 'hello'::varchar(20)")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], "hello")

    @async_test
    def testCharOut(self):
        yield from self.cursor.execute("SELECT 'hello'::char(20)")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], "hello               ")

    @async_test
    def testTextOut(self):
        yield from self.cursor.execute("SELECT 'hello'::text")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], "hello")

    @async_test
    def testIntervalOut(self):
        yield from self.cursor.execute(
            "SELECT '1 month 16 days 12 hours 32 minutes 64 seconds'"
            "::interval")
        retval = yield from self.cursor.fetchall()
        expected_value = aiopg8000.Interval(
            microseconds=(12 * 60 * 60 * 1000 * 1000) +
            (32 * 60 * 1000 * 1000) + (64 * 1000 * 1000),
            days=16, months=1)
        self.assertEqual(retval[0][0], expected_value)

        yield from self.cursor.execute("select interval '30 seconds'")
        retval = yield from self.cursor.fetchall()
        expected_value = datetime.timedelta(seconds=30)
        self.assertEqual(retval[0][0], expected_value)

        yield from self.cursor.execute("select interval '12 days 30 seconds'")
        retval = yield from self.cursor.fetchall()
        expected_value = datetime.timedelta(days=12, seconds=30)
        self.assertEqual(retval[0][0], expected_value)

    @async_test
    def testTimestampOut(self):
        yield from self.cursor.execute("SELECT '2001-02-03 04:05:06.17'::timestamp")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(
            retval[0][0], datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))

    # confirms that aiopg8000's binary output methods have the same output for
    # a data type as the PG server
    @async_test
    def testBinaryOutputMethods(self):
        methods = (
            ("float8send", 22.2),
            ("timestamp_send", datetime.datetime(2001, 2, 3, 4, 5, 6, 789)),
            ("byteasend", aiopg8000.Binary(b("\x01\x02"))),
            ("interval_send", aiopg8000.Interval(1234567, 123, 123)),)
        for method_out, value in methods:
            yield from self.cursor.execute("SELECT %s(%%s) as f1" % method_out, (value,))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(
                retval[0][0], self.db.make_params((value,))[0][2](value))

    @async_test
    def testInt4ArrayOut(self):
        yield from self.cursor.execute(
            "SELECT '{1,2,3,4}'::INT[] AS f1, "
            "'{{1,2,3},{4,5,6}}'::INT[][] AS f2, "
            "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT[][][] AS f3")
        f1, f2, f3 = yield from self.cursor.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    @async_test
    def testInt2ArrayOut(self):
        yield from self.cursor.execute(
            "SELECT '{1,2,3,4}'::INT2[] AS f1, "
            "'{{1,2,3},{4,5,6}}'::INT2[][] AS f2, "
            "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT2[][][] AS f3")
        f1, f2, f3 = yield from self.cursor.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    @async_test
    def testInt8ArrayOut(self):
        yield from self.cursor.execute(
            "SELECT '{1,2,3,4}'::INT8[] AS f1, "
            "'{{1,2,3},{4,5,6}}'::INT8[][] AS f2, "
            "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::INT8[][][] AS f3")
        f1, f2, f3 = yield from self.cursor.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    @async_test
    def testBoolArrayOut(self):
        yield from self.cursor.execute(
            "SELECT '{TRUE,FALSE,FALSE,TRUE}'::BOOL[] AS f1, "
            "'{{TRUE,FALSE,TRUE},{FALSE,TRUE,FALSE}}'::BOOL[][] AS f2, "
            "'{{{TRUE,FALSE},{FALSE,TRUE}},{{NULL,TRUE},{FALSE,FALSE}}}'"
            "::BOOL[][][] AS f3")
        f1, f2, f3 = yield from self.cursor.fetchone()
        self.assertEqual(f1, [True, False, False, True])
        self.assertEqual(f2, [[True, False, True], [False, True, False]])
        self.assertEqual(
            f3,
            [[[True, False], [False, True]], [[None, True], [False, False]]])

    @async_test
    def testFloat4ArrayOut(self):
        yield from self.cursor.execute(
            "SELECT '{1,2,3,4}'::FLOAT4[] AS f1, "
            "'{{1,2,3},{4,5,6}}'::FLOAT4[][] AS f2, "
            "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT4[][][] AS f3")
        f1, f2, f3 = yield from self.cursor.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    @async_test
    def testFloat8ArrayOut(self):
        yield from self.cursor.execute(
            "SELECT '{1,2,3,4}'::FLOAT8[] AS f1, "
            "'{{1,2,3},{4,5,6}}'::FLOAT8[][] AS f2, "
            "'{{{1,2},{3,4}},{{NULL,6},{7,8}}}'::FLOAT8[][][] AS f3")
        f1, f2, f3 = yield from self.cursor.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    @async_test
    def testIntArrayRoundtrip(self):
        # send small int array, should be sent as INT2[]
        yield from self.cursor.execute("SELECT %s as f1", ([1, 2, 3],))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], [1, 2, 3])
        column_name, column_typeoid = self.cursor.description[0][0:2]
        self.assertEqual(column_typeoid, 1005, "type should be INT2[]")

        # test multi-dimensional array, should be sent as INT2[]
        yield from self.cursor.execute("SELECT %s as f1", ([[1, 2], [3, 4]],))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], [[1, 2], [3, 4]])

        column_name, column_typeoid = self.cursor.description[0][0:2]
        self.assertEqual(column_typeoid, 1005, "type should be INT2[]")

        # a larger value should kick it up to INT4[]...
        sql = """
        SELECT %s as f1 -- integer[]
        """
        yield from self.cursor.execute(sql, ([70000, 2, 3],))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], [70000, 2, 3])
        column_name, column_typeoid = self.cursor.description[0][0:2]
        self.assertEqual(column_typeoid, 1007, "type should be INT4[]")

        # a much larger value should kick it up to INT8[]...
        sql = """
        SELECT %s as f1 -- bigint[]
        """
        yield from self.cursor.execute(
            sql, ([7000000000, 2, 3],))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(
            retval[0][0], [7000000000, 2, 3],
            "retrieved value match failed")
        column_name, column_typeoid = self.cursor.description[0][0:2]
        self.assertEqual(column_typeoid, 1016, "type should be INT8[]")

    @async_test
    def testIntArrayWithNullRoundtrip(self):
        yield from self.cursor.execute("SELECT %s as f1", ([1, None, 3],))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], [1, None, 3])

    @async_test
    def testFloatArrayRoundtrip(self):
        yield from self.cursor.execute("SELECT %s as f1", ([1.1, 2.2, 3.3],))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], [1.1, 2.2, 3.3])

    @async_test
    def testBoolArrayRoundtrip(self):
        yield from self.cursor.execute("SELECT %s as f1", ([True, False, None],))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], [True, False, None])

    @async_test
    def testStringArrayOut(self):
        yield from self.cursor.execute("SELECT '{a,b,c}'::TEXT[] AS f1")
        record = yield from self.cursor.fetchone()
        self.assertEqual(record[0], ["a", "b", "c"])
        yield from self.cursor.execute("SELECT '{a,b,c}'::CHAR[] AS f1")
        record = yield from self.cursor.fetchone()
        self.assertEqual(record[0], ["a", "b", "c"])
        yield from self.cursor.execute("SELECT '{a,b,c}'::VARCHAR[] AS f1")
        record = yield from self.cursor.fetchone()
        self.assertEqual(record[0], ["a", "b", "c"])
        yield from self.cursor.execute("SELECT '{a,b,c}'::CSTRING[] AS f1")
        record = yield from self.cursor.fetchone()
        self.assertEqual(record[0], ["a", "b", "c"])
        yield from self.cursor.execute("SELECT '{a,b,c}'::NAME[] AS f1")
        record = yield from self.cursor.fetchone()
        self.assertEqual(record[0], ["a", "b", "c"])
        yield from self.cursor.execute("SELECT '{}'::text[];")
        record = yield from self.cursor.fetchone()
        self.assertEqual(record[0], [])
        yield from self.cursor.execute("SELECT '{NULL,\"NULL\",NULL,\"\"}'::text[];")
        record = yield from self.cursor.fetchone()
        self.assertEqual(record[0], [None, 'NULL', None, ""])

    @async_test
    def testNumericArrayOut(self):
        yield from self.cursor.execute("SELECT '{1.1,2.2,3.3}'::numeric[] AS f1")

        record = yield from self.cursor.fetchone()
        self.assertEqual(
            record[0], [
                decimal.Decimal("1.1"), decimal.Decimal("2.2"),
                decimal.Decimal("3.3")])

    @async_test
    def testNumericArrayRoundtrip(self):
        v = [decimal.Decimal("1.1"), None, decimal.Decimal("3.3")]
        yield from self.cursor.execute("SELECT %s as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

    @async_test
    def testStringArrayRoundtrip(self):
        v = ["Hello!", "World!", "abcdefghijklmnopqrstuvwxyz", "",
             "A bunch of random characters:",
             " ~!@#$%^&*()_+`1234567890-=[]\\{}|{;':\",./<>?\t",
             None]
        yield from self.cursor.execute("SELECT %s as f1", (v,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], v)

    @async_test
    def testUnicodeArrayRoundtrip(self):
        if PY2:
            v = map(unicode, ("Second", "To", None))  # noqa
            yield from self.cursor.execute("SELECT %s as f1", (v,))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(retval[0][0], v)

    @async_test
    def testArrayHasValue(self):
        self.assertRaises(
            aiopg8000.ArrayContentEmptyError,
            self.db.array_inspect, [[None], [None], [None]])
        yield from self.db.rollback()

    @async_test
    def testArrayContentNotSupported(self):
        class Kajigger(object):
            pass
        self.assertRaises(
            aiopg8000.ArrayContentNotSupportedError,
            self.db.array_inspect, [[Kajigger()], [None], [None]])
        yield from self.db.rollback()

    @async_test
    def testArrayDimensions(self):
        for arr in (
                [1, [2]], [[1], [2], [3, 4]],
                [[[1]], [[2]], [[3, 4]]],
                [[[1]], [[2]], [[3, 4]]],
                [[[[1]]], [[[2]]], [[[3, 4]]]],
                [[1, 2, 3], [4, [5], 6]]):

            arr_send = self.db.array_inspect(arr)[2]
            self.assertRaises(
                aiopg8000.ArrayDimensionsNotConsistentError, arr_send, arr)
            yield from self.db.rollback()

    @async_test
    def testArrayHomogenous(self):
        arr = [[[1]], [[2]], [[3.1]]]
        arr_send = self.db.array_inspect(arr)[2]
        self.assertRaises(
            aiopg8000.ArrayContentNotHomogenousError, arr_send, arr)
        yield from self.db.rollback()

    @async_test
    def testArrayInspect(self):
        self.db.array_inspect([1, 2, 3])
        self.db.array_inspect([[1], [2], [3]])
        self.db.array_inspect([[[1]], [[2]], [[3]]])

    @async_test
    def testMacaddr(self):
        yield from self.cursor.execute("SELECT macaddr '08002b:010203'")
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], "08:00:2b:01:02:03")

    @async_test
    def testTsvectorRoundtrip(self):
        yield from self.cursor.execute(
            "SELECT cast(%s as tsvector)",
            ('a fat cat sat on a mat and ate a fat rat',))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(
            retval[0][0], "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'")

    @async_test
    def testHstoreRoundtrip(self):
        val = '"a"=>"1"'
        yield from self.cursor.execute("SELECT cast(%s as hstore)", (val,))
        retval = yield from self.cursor.fetchall()
        self.assertEqual(retval[0][0], val)

    @async_test
    def testJsonRoundtrip(self):
        if sys.version_info >= (2, 6) and \
                self.db._server_version >= LooseVersion('9.2'):
            import json
            val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
            yield from self.cursor.execute(
                "SELECT cast(%s as json)", (json.dumps(val),))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(retval[0][0], val)

    @async_test
    def testJsonbRoundtrip(self):
        if sys.version_info >= (2, 6) and \
                self.db._server_version >= LooseVersion('9.4'):
            import json
            val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
            yield from self.cursor.execute(
                "SELECT cast(%s as jsonb)", (json.dumps(val),))
            retval = yield from self.cursor.fetchall()
            self.assertEqual(retval[0][0], val)

class SimpleQueryTests(PreparedTests):
    def setUp(self):
        PreparedTests.setUp(self)
        self.db.prepared = False



if __name__ == "__main__":
    unittest.main()
