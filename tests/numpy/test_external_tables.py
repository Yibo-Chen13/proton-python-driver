try:
    import numpy as np
    import pandas as pd
except ImportError:
    np = None
    pd = None

from tests.numpy.testcase import NumpyBaseTestCase


class ExternalTablesTestCase(NumpyBaseTestCase):
    def test_select(self):
        tables = [{
            'name': 'test',
            'structure': [('x', 'int32'), ('y', 'string')],
            'data': pd.DataFrame({
                'x': [100, 500],
                'y': ['abc', 'def']
            })
        }]
        rv = self.client.execute(
            'SELECT * FROM test', external_tables=tables, columnar=True
        )
        self.assertarraysListEqual(
            rv, [np.array([100, 500]), np.array(['abc', 'def'])]
        )

    def test_send_empty_table(self):
        tables = [{
            'name': 'test',
            'structure': [('x', 'int32')],
            'data': pd.DataFrame({'x': []})
        }]
        rv = self.client.execute(
            'SELECT * FROM test', external_tables=tables, columnar=True
        )
        self.assertarraysListEqual(rv, [])

    def test_send_empty_table_structure(self):
        tables = [{
            'name': 'test',
            'structure': [],
            'data': pd.DataFrame()
        }]
        with self.assertRaises(ValueError) as e:
            self.client.execute(
                'SELECT * FROM test', external_tables=tables, columnar=True
            )

        self.assertIn('Empty table "test" structure', str(e.exception))
