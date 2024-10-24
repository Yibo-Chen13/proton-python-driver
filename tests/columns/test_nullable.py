from tests.testcase import BaseTestCase
from proton_driver import errors

ErrorCodes = errors.ErrorCodes


class nullableTestCase(BaseTestCase):
    def test_simple(self):
        columns = 'a nullable(int32)'

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

    def test_nullable_inside_nullable(self):
        columns = 'a nullable(nullable(int32))'

        with self.assertRaises(errors.ServerException) as e:
            self.client.execute(
                'CREATE STREAM test ({}) ''ENGINE = Memory'.format(columns)
            )

        self.assertEqual(e.exception.code, ErrorCodes.ILLEGAL_TYPE_OF_ARGUMENT)

    def test_nullable_array(self):
        columns = 'a nullable(array(nullable(array(nullable(int32)))))'

        with self.assertRaises(errors.ServerException) as e:
            self.client.execute(
                'CREATE STREAM test ({}) ''ENGINE = Memory'.format(columns)
            )

        self.assertEqual(e.exception.code, ErrorCodes.ILLEGAL_TYPE_OF_ARGUMENT)
