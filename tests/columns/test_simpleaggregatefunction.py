from enum import IntEnum

from tests.testcase import BaseTestCase


class SimpleAggregateFunctionTestCase(BaseTestCase):
    # required_server_version = (19, 8, 3)

    def test_simple(self):
        columns = 'a simple_aggregate_function(any, int32)'

        data = [(3, ), (2, )]
        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '3\n2\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        columns = 'a simple_aggregate_function(any, nullable(int32))'

        data = [(3, ), (None, ), (2, )]
        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '3\n\\N\n2\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_simple_agg_function(self):
        class A(IntEnum):
            hello = -1
            world = 2

        columns = "a simple_aggregate_function(any_last, " \
                  "enum8('hello' = -1, 'world' = 2))"

        data = [(A.hello,), (A.world,), (-1,), (2,)]
        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    'hello\n'
                    'world\n'
                    'hello\n'
                    'world\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    ('hello',), ('world',),
                    ('hello',), ('world',)
                ]
            )

    def test_simple_agg_function_nullable(self):
        class A(IntEnum):
            hello = -1
            world = 2

        columns = "a simple_aggregate_function(any_last, " \
                  "nullable(enum8('hello' = -1, 'world' = 2)))"

        data = [(A.hello,), (A.world,), (None,), (-1,), (2,)]
        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    'hello\n'
                    'world\n'
                    '\\N\n'
                    'hello\n'
                    'world\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    ('hello',), ('world',),
                    (None, ),
                    ('hello',), ('world',)
                ]
            )
