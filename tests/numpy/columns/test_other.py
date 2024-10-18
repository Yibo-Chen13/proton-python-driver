from parameterized import parameterized

from proton_driver import errors
from proton_driver.columns.service import get_column_by_spec
from proton_driver.context import Context

from tests.numpy.testcase import NumpyBaseTestCase


class OtherColumnsTestCase(NumpyBaseTestCase):
    def get_column(self, spec):
        ctx = Context()
        ctx.client_settings = {'strings_as_bytes': False, 'use_numpy': True}
        return get_column_by_spec(spec, {'context': ctx})

    @parameterized.expand([
        ("enum8('hello' = 1, 'world' = 2)", ),
        ('decimal(8, 4)', ),
        ('array(string)', ),
        ('tuple(string)', ),
        ('simple_aggregate_function(any, int32)', ),
        ('map(string, string)', ),
        ('array(low_cardinality(string))', )
    ])
    def test_generic_type(self, spec):
        col = self.get_column(spec)
        self.assertIsNotNone(col)

    def test_get_unknown_column(self):
        with self.assertRaises(errors.UnknownTypeError) as e:
            self.get_column('Unicorn')

        self.assertIn('Unicorn', str(e.exception))
