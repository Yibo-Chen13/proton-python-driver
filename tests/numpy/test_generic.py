import types
try:
    import numpy as np
    import pandas as pd
except ImportError:
    np = None
    pd = None

from tests.testcase import BaseTestCase
from tests.numpy.testcase import NumpyBaseTestCase


class GenericTestCase(NumpyBaseTestCase):
    n = 10

    def test_columnar(self):
        rv = self.client.execute(
            'SELECT number FROM numbers({})'.format(self.n), columnar=True
        )

        self.assertEqual(len(rv), 1)
        self.assertIsInstance(rv[0], (np.ndarray, ))

    def test_rowwise(self):
        rv = self.client.execute(
            'SELECT number FROM numbers({})'.format(self.n)
        )

        self.assertEqual(len(rv), self.n)
        self.assertIsInstance(rv[0], (np.ndarray, ))

    def test_insert_not_supported(self):
        data = [np.array(range(self.n))]

        with self.create_stream('a int32'):
            with self.assertRaises(ValueError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

            self.assertEqual(
                'NumPy inserts is only allowed with columnar=True',
                str(e.exception)
            )

    def test_with_column_types(self):
        rv = self.client.execute(
            'SELECT CAST(2 AS int32) AS x', with_column_types=True
        )

        self.assertEqual(rv, ([(2, )], [('x', 'int32')]))


class NumpyProgressTestCase(NumpyBaseTestCase):
    def test_select_with_progress(self):
        progress = self.client.execute_with_progress('SELECT 2')
        self.assertEqual(
            list(progress),
            [(1, 0), (1, 0)]
        )
        self.assertEqual(progress.get_result(), [(2,)])
        self.assertTrue(self.client.connection.connected)

    def test_select_with_progress_no_progress_obtaining(self):
        progress = self.client.execute_with_progress('SELECT 2')
        self.assertEqual(progress.get_result(), [(2,)])


class NumpyIteratorTestCase(NumpyBaseTestCase):
    def test_select_with_iter(self):
        result = self.client.execute_iter(
            'SELECT number FROM system.numbers LIMIT 10'
        )
        self.assertIsInstance(result, types.GeneratorType)

        self.assertEqual(list(result), list(zip(range(10))))
        self.assertEqual(list(result), [])

    def test_select_with_iter_with_column_types(self):
        result = self.client.execute_iter(
            'SELECT CAST(number AS uint32) as number '
            'FROM system.numbers LIMIT 10',
            with_column_types=True
        )
        self.assertIsInstance(result, types.GeneratorType)

        self.assertEqual(
            list(result),
            [[('number', 'uint32')]] + list(zip(range(10)))
        )
        self.assertEqual(list(result), [])


class DataFrameTestCase(NumpyBaseTestCase):
    def test_query_simple(self):
        df = self.client.query_dataframe(
            'SELECT CAST(number AS int64) AS x FROM system.numbers LIMIT 100'
        )

        self.assertTrue(df.equals(pd.DataFrame({'x': range(100)})))

    def test_query_replace_whitespace_in_column_names(self):
        df = self.client.query_dataframe(
            'SELECT number AS "test me" FROM system.numbers LIMIT 100'
        )

        self.assertIn('test_me', df)

    def test_insert_simple(self):
        n = 10
        df = pd.DataFrame({
            'a': range(n),
            'b': [float(x) for x in range(n)]
        })

        with self.create_stream('a int64, b float64'):
            rv = self.client.insert_dataframe('INSERT INTO test VALUES', df)
            self.assertEqual(rv, n)
            df2 = self.client.query_dataframe('SELECT * FROM test ORDER BY a')
            self.assertTrue(df.equals(df2))

    def test_insert_chunking(self):
        with self.create_stream('a int64'):
            rv = self.client.execute(
                'INSERT INTO test VALUES', [np.array(range(3))], columnar=True,
                settings={'insert_block_size': 1}
            )
            self.assertEqual(rv, 3)

    def test_insert_not_ordered_columns(self):
        n = 10
        df = pd.DataFrame({
            'b': range(n),
            'a': [str(x) for x in range(n)]
        })[['b', 'a']]

        with self.create_stream('a string, b float64'):
            rv = self.client.insert_dataframe(
                'INSERT INTO test (a, b) VALUES', df
            )
            self.assertEqual(rv, n)


class NoNumPyTestCase(BaseTestCase):
    def setUp(self):
        super(NoNumPyTestCase, self).setUp()

        try:
            import numpy  # noqa: F401
            import pandas  # noqa: F401
        except Exception:
            pass

        else:
            self.skipTest('NumPy extras are installed')

    def test_runtime_error_without_numpy(self):
        with self.assertRaises(RuntimeError) as e:
            with self.created_client(settings={'use_numpy': True}) as client:
                client.execute('SELECT 1')

        self.assertEqual(
            'Extras for NumPy must be installed', str(e.exception)
        )

    def test_query_dataframe(self):
        with self.assertRaises(RuntimeError) as e:
            with self.created_client(settings={'use_numpy': True}) as client:
                client.query_dataframe('SELECT 1 AS x')

        self.assertEqual(
            'Extras for NumPy must be installed', str(e.exception)
        )
