import dataclasses
from datetime import datetime, date
from unittest import TestCase

from django_csv import columns
from django_csv.mcsv import Csv


class CsvTest(TestCase):
    def test_columns(self):
        @dataclasses.dataclass
        class TestClass:
            attribute: str

        class DefaultUseCsv(Csv):
            write_only_static = columns.WriteOnlyStaticColumn(
                index=0, header='write only static')
            static = columns.StaticColumn(
                index=1, static_value='static', header='static')
            insert_static = columns.StaticColumn(
                index=2, header='insert static')

            attribute = columns.AttributeColumn(index=3, header='attribute')
            method = columns.MethodColumn(index=4, header='method')

            def column_method(self, instance: TestClass, **kwargs) -> str:
                return instance.attribute + '\'s method'

            def field_calc(self, values: dict, **kwargs) -> str:
                return values['write_only_static'] + ' calc'

            def field_set_static(self, static: dict, **kwargs):
                return static['set']

            def field(self, values: dict, static: dict, **kwargs) -> dict:
                values['field'] = f'field {values["method"]} {static["set"]}'
                return values

        queryset = [TestClass(attribute=f'attribute {i}') for i in range(3)]
        for_write = DefaultUseCsv.for_write(queryset=queryset)
        self.assertListEqual(
            for_write.get_headers(),
            ['write only static', 'static', 'insert static', 'attribute', 'method']
        )
        for_write.set_static_column('insert_static', 'inserted')

        for i, row in enumerate(for_write.get_table(header=False)):
            with self.subTest(i):
                attr = f'attribute {i}'
                self.assertListEqual(
                    row, ['', 'static', 'inserted', attr, f'{attr}\'s method'])

        table = [[f'{x}_{y}' for x in range(5)] for y in range(3)]

        for_read = DefaultUseCsv.for_read(table=table)
        for_read.set_static('set', 'set static')
        for_read.set_static_column('insert_static', 'inserted')

        for y, row_values in enumerate(for_read.get_as_dict()):
            with self.subTest(y):
                # WriteOnlyStaticColumn returns a value from a row.
                self.assertEqual(row_values['write_only_static'], f'0_{y}')
                # StaticColumn always returns a static value.
                self.assertEqual(row_values['static'], 'static')
                self.assertEqual(row_values['insert_static'], 'inserted')
                # AttributeColumn and MethodColumn return values from rows.
                self.assertEqual(row_values['attribute'], f'3_{y}')
                self.assertEqual(row_values['method'], f'4_{y}')

                # values are set from field_<key> method.
                self.assertEqual(row_values['calc'], f'0_{y} calc')
                self.assertEqual(row_values['set_static'], 'set static')
                self.assertEqual(row_values['field'], f'field 4_{y} set static')


class CsvMetaOptionTest(TestCase):
    def test_csv_validation(self):

        class OverWrapIndexCsv(Csv):
            var1 = columns.StaticColumn(index=0)
            var2 = columns.AttributeColumn(index=1)
            var3 = columns.MethodColumn(index=1)

        with self.assertRaises(ValueError):
            OverWrapIndexCsv.for_read(table=[[]])

        with self.assertRaises(ValueError):
            OverWrapIndexCsv.for_write(queryset=[])

        class RaiseExceptionOnlyForWriteCsv(Csv):
            var1 = columns.StaticColumn()  # raise Error only for_read
            var2 = columns.AttributeColumn(index=1)
            var3 = columns.MethodColumn(index=2)

        try:
            RaiseExceptionOnlyForWriteCsv.for_read(table=[
                ['', '', ''], ['', '', '']])
        except columns.ColumnValidationError:
            self.fail('`RaiseExceptionOnlyForWriteCsv` raise'
                      'ColumnValidationError unexpectedly')

        with self.assertRaises(columns.ColumnValidationError):
            RaiseExceptionOnlyForWriteCsv.for_write(queryset=[])

        class RaiseExceptionCsv(Csv):
            var1 = columns.StaticColumn(index=0)
            var2 = columns.AttributeColumn()
            var3 = columns.MethodColumn(index=2)

        with self.assertRaises(columns.ColumnValidationError):
            RaiseExceptionCsv.for_write(queryset=[])

        with self.assertRaises(columns.ColumnValidationError):
            RaiseExceptionCsv.for_read(table=[
                ['', '', ''], ['', '', '']])

        class AutoAssignCsv(RaiseExceptionCsv):
            class Meta:
                auto_assign = True

        try:
            AutoAssignCsv.for_write(queryset=[])
        except columns.ColumnValidationError:
            self.fail('`AutoAssignCsv` raise'
                      'ColumnValidationError unexpectedly')

        try:
            AutoAssignCsv.for_read(table=[
                ['', '', ''], ['', '', '']])
        except columns.ColumnValidationError:
            self.fail('`AutoAssignCsv` raise'
                      'ColumnValidationError unexpectedly')

    def test_mode_regulation(self):

        class ReadOnlyCsv(Csv):
            static = columns.StaticColumn()
            method = columns.MethodColumn()
            attribute = columns.AttributeColumn()

            class Meta:
                read_mode = True
                write_mode = False
                auto_assign = True

        try:
            ReadOnlyCsv.for_read(table=[[i for i in range(3)] for _ in range(5)])
        except ReadOnlyCsv.ReadModeIsProhibited:
            self.fail('`ReadOnlyCsv` raise `ReadModeIsProhibited` unexpectedly')

        with self.assertRaises(ReadOnlyCsv.WriteModeIsProhibited):
            ReadOnlyCsv.for_write(queryset=[])

        class WriteOnlyCsv(Csv):
            static = columns.StaticColumn()
            method = columns.MethodColumn()
            attribute = columns.AttributeColumn()

            class Meta:
                read_mode = False
                write_mode = True
                auto_assign = True

        with self.assertRaises(WriteOnlyCsv.ReadModeIsProhibited):
            WriteOnlyCsv.for_read(table=[[i for i in range(3)] for _ in range(5)])

        try:
            WriteOnlyCsv.for_write(queryset=[])
        except WriteOnlyCsv.WriteModeIsProhibited:
            self.fail('`WriteOnlyCsv` raises `WriteModeIsProhibited` unexpectedly')

        class OverrideReadOnlyCsv(ReadOnlyCsv):
            pass

        try:
            OverrideReadOnlyCsv.for_read(
                table=[[i for i in range(3)] for _ in range(5)])
        except OverrideReadOnlyCsv.ReadModeIsProhibited:
            self.fail('`OverrideReadOnlyCsv` raise '
                      '`ReadModeIsProhibited` unexpectedly')

        with self.assertRaises(OverrideReadOnlyCsv.WriteModeIsProhibited):
            OverrideReadOnlyCsv.for_write(queryset=[])

        class OverrideWriteOnlyCsv(WriteOnlyCsv):
            pass

        with self.assertRaises(OverrideWriteOnlyCsv.ReadModeIsProhibited):
            OverrideWriteOnlyCsv.for_read(
                table=[[i for i in range(3)] for _ in range(5)])

        try:
            OverrideWriteOnlyCsv.for_write(queryset=[])
        except OverrideWriteOnlyCsv.WriteModeIsProhibited:
            self.fail('`OverrideWriteOnlyCsv` raises '
                      '`WriteModeIsProhibited` unexpectedly')

        class ReadAndWriteCsv(OverrideReadOnlyCsv, OverrideWriteOnlyCsv):
            class Meta:
                read_mode = True
                write_mode = True

        try:
            ReadAndWriteCsv.for_read(
                table=[[i for i in range(3)] for _ in range(5)])
        except ReadAndWriteCsv.ReadModeIsProhibited:
            self.fail('`ReadAndWriteCsv` raise '
                      '`ReadModeIsProhibited` unexpectedly')

        try:
            ReadAndWriteCsv.for_write(queryset=[])
        except ReadAndWriteCsv.WriteModeIsProhibited:
            self.fail('`ReadAndWriteCsv` raises '
                      '`WriteModeIsProhibited` unexpectedly')

    def test_meta_convert(self):
        class DefaultConvertCsv(Csv):
            false = columns.MethodColumn(index=0, to=bool)
            true = columns.MethodColumn(index=1, to=bool)
            date_time = columns.MethodColumn(index=2, to=datetime)
            date_ = columns.MethodColumn(index=3, to=date)

            def column_false(self, **kwargs):
                return False

            def column_true(self, **kwargs):
                return True

            def column_date_time(self, **kwargs):
                return datetime(2022, 6, 25)

            def column_date_(self, **kwargs):
                return date(2022, 6, 25)

        input_row = ['no', 'yes', '2022-06-25 00:00:00', '2022-06-25']

        boolean_for_read = DefaultConvertCsv.for_read(
            table=[input_row for _ in range(5)])

        expected_result_for_read = {
            'false': False, 'true': True,
            'date_time': datetime(2022, 6, 25),
            'date_': date(2022, 6, 25),
        }

        values = list(boolean_for_read.get_as_dict())[0]
        self.assertDictEqual(values, expected_result_for_read)

        boolean_for_write = DefaultConvertCsv.for_write(queryset=[1])
        row = boolean_for_write.get_table(header=False)[0]
        self.assertListEqual(row, input_row)

        class ShowBooleanCsv(DefaultConvertCsv):
            false2 = columns.MethodColumn(index=4, to=bool)
            true2 = columns.MethodColumn(index=5, to=bool)

            class Meta:
                show_true = 'Show True'
                show_false = 'Show False'
                as_true = ['True', 'true']
                as_false = ['False', 'false']
                datetime_format = '%y/%m/%d %H:%M:%S'
                date_format = '%y/%m/%d'

            def column_false2(self, **kwargs):
                return False

            def column_true2(self, **kwargs):
                return True

        input_row = [
            'False', 'True', '22/06/25 00:00:00', '22/06/25', 'false', 'true']

        boolean_for_read = ShowBooleanCsv.for_read(
            table=[input_row for _ in range(5)])

        expected_result_for_read |= {'false2': False, 'true2': True}
        values = list(boolean_for_read.get_as_dict())[0]
        self.assertDictEqual(values, expected_result_for_read)

        boolean_for_write = ShowBooleanCsv.for_write(queryset=[1])
        row = boolean_for_write.get_table(header=False)[0]
        self.assertListEqual(
            row, ['Show False', 'Show True', '22/06/25 00:00:00', '22/06/25', 'Show False', 'Show True']
        )

        class OverrideShowBooleanCsv(ShowBooleanCsv):
            class Meta:
                auto_convert = False

        input_row = [
            'False', 'True', '22/06/25 00:00:00', '22/06/25', 'false', 'true']

        boolean_for_read = OverrideShowBooleanCsv.for_read(
            table=[input_row for _ in range(5)])
        values = list(boolean_for_read.get_as_dict())[0]
        self.assertDictEqual(values, {
            'false': 'False', 'true': 'True', 'date_time': '22/06/25 00:00:00',
            'date_': '22/06/25', 'false2': 'false', 'true2': 'true',
        })

        boolean_for_write = OverrideShowBooleanCsv.for_write(queryset=[1])
        row = boolean_for_write.get_table(header=False)[0]
        self.assertListEqual(row, [
            'False', 'True', '2022-06-25 00:00:00', '2022-06-25', 'False', 'True'
        ])

    def test_insert_blank_column(self):
        class NotPaddingCsv(Csv):
            zero = columns.StaticColumn(static_value=0, index=0)
            one = columns.StaticColumn(static_value=1, index=1)
            three = columns.StaticColumn(static_value=3, index=3)

        for_write = NotPaddingCsv.for_write(queryset=[1])
        row = for_write.get_table(header=False)[0]
        self.assertEqual(len(row), 4)
        self.assertListEqual(row, ['0', '1', '', '3'])

        class PaddingCsv(NotPaddingCsv):
            class Meta:
                insert_blank_column = False

        for_write = PaddingCsv.for_write(queryset=[1])
        row = for_write.get_table(header=False)[0]
        self.assertEqual(len(row), 3)
        self.assertListEqual(row, ['0', '1', '3'])
