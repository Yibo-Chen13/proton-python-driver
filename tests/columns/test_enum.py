from enum import IntEnum

from tests.testcase import BaseTestCase
from proton_driver import errors


class A(IntEnum):
    hello = -1
    world = 2


class B(IntEnum):
    foo = -300
    bar = 300


class EnumTestCase(BaseTestCase):
    def test_simple(self):
        columns = (
            "a enum8('hello' = -1, 'world' = 2), "
            "b enum16('foo' = -300, 'bar' = 300)"
        )

        data = [(A.hello, B.bar), (A.world, B.foo), (-1, 300), (2, -300)]
        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    'hello\tbar\n'
                    'world\tfoo\n'
                    'hello\tbar\n'
                    'world\tfoo\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    ('hello', 'bar'), ('world', 'foo'),
                    ('hello', 'bar'), ('world', 'foo')
                ]
            )

    def test_enum_by_string(self):
        columns = "a enum8('hello' = 1, 'world' = 2)"
        data = [('hello', ), ('world', )]
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
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_errors(self):
        columns = "a enum8('test' = 1, 'me' = 2)"
        data = [(A.world, )]
        with self.create_stream(columns):
            with self.assertRaises(errors.LogicalError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

        columns = "a enum8('test' = 1, 'me' = 2)"
        data = [(3, )]
        with self.create_stream(columns):
            with self.assertRaises(errors.LogicalError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_quote_in_name(self):
        columns = "a enum8(' \\' t = ' = -1, 'test' = 2)"
        data = [(-1, ), (" ' t = ", )]
        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    " \\' t = \n"
                    " \\' t = \n"
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(" ' t = ", ), (" ' t = ", )])

    def test_comma_and_space_in_name(self):
        columns = "a enum8('one' = 1, 'two_with_comma, ' = 2, 'three' = 3)"
        data = [(2, ), ('two_with_comma, ', )]
        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)

            self.assertEqual(
                inserted, (
                    'two_with_comma, \n'
                    'two_with_comma, \n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [('two_with_comma, ',), ('two_with_comma, ',)]
            )

    def test_nullable(self):
        columns = "a nullable(enum8('hello' = -1, 'world' = 2))"

        data = [(None, ), (A.hello, ), (None, ), (A.world, )]
        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    '\\N\nhello\n\\N\nworld\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (None, ), ('hello', ), (None, ), ('world', ),
                ]
            )
