from datetime import date

from proton_driver import errors
from tests.testcase import BaseTestCase


class TupleTestCase(BaseTestCase):
    def entuple(self, lst):
        return tuple(
            self.entuple(x) if isinstance(x, list) else x for x in lst
        )

    def test_simple(self):
        columns = 'a tuple(int32, string)'
        data = [((1, 'a'), ), ((2, 'b'), )]

        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, "(1,'a')\n(2,'b')\n")

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_tuple_single_element(self):
        columns = 'a tuple(int32)'
        data = [((1, ), ), ((2, ), )]

        with self.create_stream(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, "(1)\n(2)\n")

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        with self.create_stream('a tuple(nullable(int32), nullable(string))'):
            data = [
                ((1, 'a'), ),
                ((2, None), ), ((None, None), ), ((None, 'd'), ),
                ((5, 'e'), )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "(1,'a')\n"
                "(2,NULL)\n(NULL,NULL)\n(NULL,'d')\n"
                "(5,'e')\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nested_tuple_with_common_types(self):
        columns = 'a tuple(string, tuple(int32, string), string)'

        with self.create_stream(columns):
            data = [
                (('one', (1, 'a'), 'two'), ),
                (('three', (2, 'b'), 'four'), )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "('one',(1,'a'),'two')\n"
                "('three',(2,'b'),'four')\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_tuple_of_tuples(self):
        columns = (
            "a tuple("
            "tuple(int32, string),"
            "tuple(enum8('hello' = 1, 'world' = 2), Date)"
            ")"
        )

        with self.create_stream(columns):
            data = [
                (((1, 'a'), (1, date(2020, 3, 11))), ),
                (((2, 'b'), (2, date(2020, 3, 12))), )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "((1,'a'),('hello','2020-03-11'))\n"
                "((2,'b'),('world','2020-03-12'))\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (((1, 'a'), ('hello', date(2020, 3, 11))), ),
                    (((2, 'b'), ('world', date(2020, 3, 12))), )
                ]
            )

    def test_tuple_of_arrays(self):
        with self.create_stream('a tuple(array(int32))'):
            data = [(([1, 2, 3], ), ), (([4, 5, 6], ), )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "([1,2,3])\n([4,5,6])\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    # Bug in array of Tuple handing before 19.16.13:
    # DESCRIBE STREAM test
    #
    # | a.1  | array(uint8) |
    # | a.2  | array(uint8) |
    # | a.3  | array(uint8) |
    # https://github.com/ClickHouse/ClickHouse/pull/8866
    # @require_server_version(19, 16, 13)
    def test_array_of_tuples(self):
        with self.create_stream('a array(tuple(uint8, uint8, uint8))'):
            data = [
                ([(1, 2, 3), (4, 5, 6)], ),
                ([(7, 8, 9)],),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "[(1,2,3),(4,5,6)]\n"
                "[(7,8,9)]\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_type_mismatch_error(self):
        columns = 'a tuple(int32)'
        data = [('test', )]

        with self.create_stream(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute('INSERT INTO test (a) VALUES', data)
