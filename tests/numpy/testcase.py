try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.util import check_numpy
from tests.testcase import BaseTestCase


class NumpyBaseTestCase(BaseTestCase):
    client_kwargs = {'settings': {'use_numpy': True}}

    @check_numpy
    def setUp(self):
        super(NumpyBaseTestCase, self).setUp()

    def assertarraysEqual(self, first, second):
        return self.assertTrue((first == second).all())

    def assertarraysListEqual(self, first, second):
        self.assertEqual(len(first), len(second))
        for x, y in zip(first, second):
            self.assertTrue((x == y).all())
