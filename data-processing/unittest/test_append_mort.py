import unittest
from append_mortality_data import AppendMortalityData

class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.d = AppendMortalityData()

    def test_something(self):
        states = self.d.get_states(self.d)
        states.show(60)
        self.assertEqual(False, False)

    def test_process_year(self):
        df = self.d.process_year(self.d, 2003)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
