#  flake8: NOQA
from unittest import TestCase

from ..model_csv.columns import StaticColumn, MethodColumn, ColumnValidationError
from ..model_csv.csv.metaclasses import CsvOptions


class OptionsTest(TestCase):
    def setUp(self) -> None:
        self.columns = {
            # s => StaticColumn
            # i => has an index
            # v => read or write value = True
            # x => not static, not have an index, value = False
            's_i_v': StaticColumn(index=0),  # True, True (for_read, for_write)
            'x_i_v': MethodColumn(index=0),  # True, True
            's_x_v': StaticColumn(),  # True, False
            'x_x_v': MethodColumn(),  # False, False
            's_i_x': StaticColumn(index=1, read_value=False, write_value=False),  # False, False
            'x_i_x': MethodColumn(index=2, read_value=False, write_value=False),  # False, False
            's_x_x': StaticColumn(read_value=False, write_value=False),  # False, False
            'x_x_x': MethodColumn(read_value=False, write_value=False)  # False, False
        }
        self.meta = type('Meta', (), {})

    def test_get_column(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        self.assertEqual('s_i_v', opt.get_column('s_i_v').name)

    def test_get_columns(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        self.assertEqual(8, len(opt.get_columns()))

    def test_get_columns_is_static(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        self.assertTrue(all([col.name.startswith('s_')
                             for col in opt.get_columns(is_static=True)]))

        self.assertTrue(all([col.name.startswith('x_')
                             for col in opt.get_columns(is_static=False)]))

    def test_get_columns_has_value(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        self.assertTrue(all([col.name.endswith('_v')
                             for col in opt.get_columns(read_value=True)]))

        self.assertTrue(all([col.name.endswith('_v')
                             for col in opt.get_columns(write_value=True)]))

        self.assertTrue(all([col.name.endswith('_x')
                             for col in opt.get_columns(read_value=False)]))

        self.assertTrue(all([col.name.endswith('_x')
                             for col in opt.get_columns(write_value=False)]))

    def test_get_columns_read_index(self):

        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])

        self.assertTrue(all(['_i_' in col.name for col in
                        opt.get_columns(r_index=True, original=True)]))
        self.assertTrue(all(['_i_' in col.name for col in
                        opt.get_columns(r_index=True)]))

        self.assertEqual(6, len(opt.get_columns(r_index=False, original=True)))
        self.assertEqual(6, len(opt.get_columns(r_index=False)))

        # override r_index
        col = opt.get_column('s_x_v')
        col.r_index = 3
        self.assertFalse('s_x_v' in [
            col.name for col in opt.get_columns(r_index=True, original=True)])

        self.assertTrue('s_x_v' in [
            col.name for col in opt.get_columns(r_index=False, original=True)])

        self.assertTrue('s_x_v' in [
            col.name for col in opt.get_columns(r_index=True)])

        self.assertFalse('s_x_v' in [
            col.name for col in opt.get_columns(r_index=False)])

    def test_get_columns_write_index(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])

        self.assertTrue(all(['_i_' in col.name for col in
                             opt.get_columns(w_index=True, original=True)]))
        self.assertTrue(all(['_i_' in col.name for col in
                             opt.get_columns(w_index=True)]))
        self.assertEqual(6, len(opt.get_columns(w_index=False, original=True)))
        self.assertEqual(6, len(opt.get_columns(w_index=False)))

        # override w_index
        col = opt.get_column('s_x_v')
        col.w_index = 3
        self.assertFalse('s_x_v' in [col.name for col in
                                     opt.get_columns(w_index=True,
                                                     original=True)])
        self.assertTrue('s_x_v' in [col.name for col in
                                    opt.get_columns(w_index=False,
                                                    original=True)])

        self.assertTrue('s_x_v' in [col.name for col in
                                    opt.get_columns(w_index=True)])
        self.assertFalse('s_x_v' in [col.name for col in
                                     opt.get_columns(w_index=False)])

    def test_get_columns_complicated(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        cols = opt.get_columns(is_static=True, r_index=True, read_value=True)
        self.assertEqual(1, len(cols))
        self.assertEqual('s_i_v', cols[0].name)

        cols = opt.get_columns(is_static=False, r_index=True, read_value=True)
        self.assertEqual(1, len(cols))
        self.assertEqual('x_i_v', cols[0].name)

        cols = opt.get_columns(is_static=True, r_index=False, read_value=True)
        self.assertEqual(1, len(cols))
        self.assertEqual('s_x_v', cols[0].name)

        cols = opt.get_columns(is_static=False, r_index=False, read_value=True)
        self.assertEqual(1, len(cols))
        self.assertEqual('x_x_v', cols[0].name)

        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        cols = opt.get_columns(is_static=True, r_index=True, read_value=False)
        self.assertEqual(0, len(cols))

        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        cols = opt.get_columns(is_static=True, r_index=False, read_value=False)
        self.assertEqual(2, len(cols))
        self.assertEqual({'s_i_x', 's_x_x'}, {col.name for col in cols})

        cols = opt.get_columns(is_static=False, r_index=True, read_value=False)
        self.assertEqual(0, len(cols))

        cols = opt.get_columns(is_static=False, r_index=False, read_value=False)
        self.assertEqual(2, len(cols))
        self.assertEqual({'x_x_x', 'x_i_x'}, {col.name for col in cols})

    def test_get_columns_for_read(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        self.assertSetEqual(
            {'s_i_v', 'x_i_v', 's_x_v'},
            set(col.name for col in opt.get_columns(for_read=True))
        )

        self.assertSetEqual(
            {'x_x_v', 's_i_x', 'x_i_x', 's_x_x', 'x_x_x'},
            set(col.name for col in opt.get_columns(for_read=False))
        )

        with self.assertRaises(TypeError):
            opt.get_columns(for_read=True, is_static=True)

        with self.assertRaises(TypeError):
            opt.get_columns(for_read=True, original=True)

        self.assertSetEqual(
            {'s_i_v', 'x_i_v'},
            set(col.name for col in opt.get_columns(for_write=True))
        )

        self.assertSetEqual(
            {'s_x_v', 'x_x_v', 's_i_x', 'x_i_x', 's_x_x', 'x_x_x'},
            set(col.name for col in opt.get_columns(for_write=False))
        )

        with self.assertRaises(TypeError):
            opt.get_columns(for_write=True, is_static=True)

        with self.assertRaises(TypeError):
            opt.get_columns(for_write=True, original=True)

    def test_get_indexes(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        self.assertEqual(2, len(opt.get_r_indexes()))
        self.assertEqual(2, len(opt.get_w_indexes()))
        self.assertSetEqual({0, }, set(opt.get_r_indexes()))
        self.assertSetEqual({0, }, set(opt.get_w_indexes()))

    def test_auto_assign(self):
        opt = CsvOptions(meta=self.meta, columns=self.columns, parts=[])
        opt.assign_number()
        self.assertEqual(len(opt.get_columns(r_index=True)), 4)
        self.assertEqual(len(opt.get_columns(r_index=True, original=True)), 2)
        self.assertSetEqual(set(opt.get_r_indexes()), {0, 1, 2})
        self.assertEqual(len(opt.get_columns(w_index=True)), 4)
        self.assertEqual(len(opt.get_columns(w_index=True, original=True)), 2)
        self.assertSetEqual(set(opt.get_w_indexes()), {0, 1, 2})

        for column in opt.get_columns(r_index=True):
            if column.name.startswith('s_'):
                column.validate_for_read()
                continue

            if column.name.endswith('_v'):
                column.validate_for_read()
                continue

            if column.r_index is not None:
                try:
                    column.validate_for_read()
                    continue
                except ColumnValidationError:
                    self.fail(f'{column.name} raise Exception Unexpectedly')

            with self.subTest(mes=column.name):
                with self.assertRaises(ColumnValidationError):
                    column.validate_for_read()

        for column in opt.get_columns(w_index=True):
            if column.name.endswith('_v'):
                column.validate_for_write()
                continue

            if column.w_index is not None:
                try:
                    column.validate_for_write()
                    continue
                except ColumnValidationError:
                    self.fail(f'{column.name} raise Exception Unexpectedly')

            with self.subTest(mes=column.name):
                with self.assertRaises(ColumnValidationError):
                    column.validate_for_write()
