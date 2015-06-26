import aiopg8000
import asyncio
from aiopg8000.tests.connection_settings import db_connect
from contextlib import closing

@asyncio.coroutine
def run():
    db = yield from aiopg8000.connect(**db_connect)
    try:
        for i in range(100):
            cursor = yield from db.cursor()
            yield from  cursor.execute("""
                SELECT n.nspname as "Schema",
                  pg_catalog.format_type(t.oid, NULL) AS "Name",
                    pg_catalog.obj_description(t.oid, 'pg_type') as "Description"
                    FROM pg_catalog.pg_type t
                         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                         left join pg_catalog.pg_namespace kj on n.oid = t.typnamespace
                         WHERE (t.typrelid = 0
                            OR (SELECT c.relkind = 'c'
                                FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
                            AND NOT EXISTS(
                                SELECT 1 FROM pg_catalog.pg_type el
                                WHERE el.oid = t.typelem AND el.typarray = t.oid)
                             AND pg_catalog.pg_type_is_visible(t.oid)
                             ORDER BY 1, 2;""")
    finally:
        yield from db.close()


asyncio.get_event_loop().run_until_complete(run())
