import unittest
from clean_mortality_data import CleanMortalityData

class MyTestCase(unittest.TestCase):

    def setUp(self):
        self.clmd = CleanMortalityData()

    #def test_2003(self):
    #    self.assertEqual(self.clmd.load_schema(2003), 107)

    def test_main_2003(self):
        self.assertEqual(self.clmd.main(self.clmd, 2003))


if __name__ == '__main__':
    unittest.main()
