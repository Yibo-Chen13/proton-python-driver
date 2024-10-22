from tests.testcase import BaseTestCase
from proton_driver import errors


class BoolTestCase(BaseTestCase):
    # required_server_version = (21, 12)

    def test_simple(self):
        columns = ("a bool")

        data = [(1,), (0,), (True,), (False,), (None,), ("False",), ("",)]
        with self.create_stream(columns):
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    'true\n'
                    'false\n'
                    'true\n'
                    'false\n'
                    'false\n'
                    'true\n'
                    'false\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (True, ),
                    (False, ),
                    (True, ),
                    (False, ),
                    (False, ),
                    (True, ),
                    (False, ),
                ]
            )

    def test_errors(self):
        columns = "a bool"
        with self.create_stream(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(1, )],
                    types_check=True
                )

    def test_nullable(self):
        columns = "a nullable(bool)"

        data = [(None, ), (True, ), (False, )]
        with self.create_stream(columns):
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    '\\N\ntrue\nfalse\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (None, ), (True, ), (False, ),
                ]
            )
