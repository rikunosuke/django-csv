import os
from unittest import TestCase

from ..model_csv import readers

TEST_DATA_DIR = os.path.dirname(__file__) + '/test_data'


class ReaderTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.expected = [
            ['String 1', '1', '1.1', '2022/07/19', '2022/07/19 10:00:00'],
            ['String 2', '2', '2.2', '2022/07/20', '2022/07/20 10:00:00'],
            ['String 3', '3', '3.3', '2022/07/21', '2022/07/21 10:00:00'],
            ['String 4', '4', '4.4', '2022/07/22', '2022/07/22 10:00:00'],
            ['String 5', '5', '5.5', '2022/07/23', '2022/07/23 10:00:00'],
        ]

    def test_csv_reader(self):
        with open(f'{TEST_DATA_DIR}/CsvTestData.csv', 'r') as f:
            table = readers.CsvReader(file=f, table_starts_from=1).get_table()

        for y, (expected_row, row) in enumerate(zip(self.expected, table)):
            for x, (expected_cell, cell) in enumerate(zip(expected_row, row)):
                with self.subTest(f'x: {x}, y: {y}'):
                    self.assertEqual(expected_cell, cell)

    def test_tsv_reader(self):
        with open(f'{TEST_DATA_DIR}/TsvTestData.tsv', 'r') as f:
            table = readers.TsvReader(file=f, table_starts_from=1).get_table()

        for y, (expected_row, row) in enumerate(zip(self.expected, table)):
            for x, (expected_cell, cell) in enumerate(zip(expected_row, row)):
                with self.subTest(f'x: {x}, y: {y}'):
                    self.assertEqual(expected_cell, cell)

    def test_xlsx_reader(self):
        with open(f'{TEST_DATA_DIR}/XlsxTestData.xlsx', 'br') as f:
            table = readers.XlsxReader(
                file=f, table_starts_from=1, date_format='%Y/%m/%d',
                datetime_format='%Y/%m/%d %H:%M:%S'
            ).get_table()

        for y, (expected_row, row) in enumerate(zip(self.expected, table)):
            for x, (expected_cell, cell) in enumerate(zip(expected_row, row)):
                with self.subTest(f'x: {x}, y: {y}'):
                    self.assertEqual(expected_cell, cell)
