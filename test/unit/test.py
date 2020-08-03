import unittest

from pmap.pmap import _get_full_sources
from .unneccesary_math_1 import map_test


class TestGetFullSource(unittest.TestCase):
    def test_get_full_sources(self):
        print(_get_full_sources(map_test))


if __name__ == '__main__':
    unittest.main()
