import dataclasses
from datetime import datetime, date, timedelta
from django.utils import timezone
from unittest import TestCase

from ..model_csv import columns
from ..model_csv import Csv, ValidationError


class CsvTest(TestCase):
    def test_columns(self):
        @dataclasses.dataclass
        class TestClass:
            attribute: str

        class DefaultUseCsv(Csv):
            static = columns.StaticColumn(
                index=0, static_value='static', header='static')
            insert_static = columns.StaticColumn(
                index=1, header='insert static')

            attribute = columns.AttributeColumn(index=2, header='attribute')
            method = columns.MethodColumn(index=3, header='method')

            def column_method(self, instance: TestClass, **kwargs) -> str:
                return instance.attribute + '\'s method'

            def field_calc(self, values: dict, **kwargs) -> str:
                return values['static'] + ' calc'

            def field_set_static(self, static: dict, **kwargs):
                return static['set']

            def field(self, values: dict, static: dict, **kwargs) -> dict:
                values['field'] = f'field {values["method"]} {static["set"]}'
                return values

        instances = [TestClass(attribute=f'attribute {i}') for i in range(3)]
        for_write = DefaultUseCsv.for_write(instances=instances)
        self.assertListEqual(
            for_write.get_headers(),
            ['static', 'insert static', 'attribute', 'method']
        )
        for_write.set_static_column('insert_static', 'inserted')

        for i, row in enumerate(for_write.get_table(header=False)):
            with self.subTest(i):
                attr = f'attribute {i}'
                self.assertListEqual(
                    row, ['static', 'inserted', attr, f'{attr}\'s method'])

        table = [[f'{x}_{y}' for x in range(4)] for y in range(3)]

        for_read = DefaultUseCsv.for_read(table=table)
        for_read.set_static('set', 'set static')
        for_read.set_static_column('insert_static', 'inserted')
        self.assertTrue(for_read.is_valid())

        for y, row in enumerate(for_read.cleaned_rows):
            with self.subTest(y):
                # Columns get values from table.
                self.assertEqual(row['static'], f'0_{y}')
                self.assertEqual(row['insert_static'], f'1_{y}')
                self.assertEqual(row['attribute'], f'2_{y}')
                self.assertEqual(row['method'], f'3_{y}')

                # values are set from field_<key> method.
                self.assertEqual(row['calc'], f'0_{y} calc')
                self.assertEqual(row['set_static'], 'set static')
                self.assertEqual(row['field'], f'field 3_{y} set static')


class CsvMetaOptionTest(TestCase):
    def test_csv_validation(self):
        class NoColumnCsv(Csv):
            pass

        with self.assertRaises(columns.ColumnValidationError):
            NoColumnCsv.for_read(table=[[0]])

        with self.assertRaises(columns.ColumnValidationError):
            NoColumnCsv.for_write(instances=[0])

        class OverWrapIndexCsv(Csv):
            var1 = columns.StaticColumn(index=0)
            var2 = columns.AttributeColumn(index=1)
            var3 = columns.MethodColumn(index=1)

        with self.assertRaises(columns.ColumnValidationError):
            OverWrapIndexCsv.for_read(table=[[]])

        with self.assertRaises(columns.ColumnValidationError):
            OverWrapIndexCsv.for_write(instances=[])

        class RaiseExceptionOnlyForWriteCsv(Csv):
            var1 = columns.StaticColumn(w_index=0)  # raise Error only for_read
            var2 = columns.AttributeColumn(index=1)
            var3 = columns.MethodColumn(index=2)

        with self.assertRaises(columns.ColumnValidationError):
            RaiseExceptionOnlyForWriteCsv.for_read(table=[
                ['', '', ''], ['', '', '']])

        try:
            RaiseExceptionOnlyForWriteCsv.for_write(instances=[])
        except columns.ColumnValidationError:
            self.fail('`RaiseExceptionOnlyForWriteCsv` raise'
                      'ColumnValidationError unexpectedly')

        class RaiseExceptionCsv(Csv):
            var1 = columns.StaticColumn(index=0)
            var2 = columns.AttributeColumn()
            var3 = columns.MethodColumn(index=2)

        with self.assertRaises(columns.ColumnValidationError):
            RaiseExceptionCsv.for_write(instances=[])

        with self.assertRaises(columns.ColumnValidationError):
            RaiseExceptionCsv.for_read(table=[
                ['', '', ''], ['', '', '']])

        class AutoAssignCsv(RaiseExceptionCsv):
            class Meta:
                auto_assign = True

        try:
            AutoAssignCsv.for_write(instances=[])
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
            ReadOnlyCsv.for_write(instances=[])

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
            WriteOnlyCsv.for_write(instances=[])
        except WriteOnlyCsv.WriteModeIsProhibited:
            self.fail('`WriteOnlyCsv` raises `WriteModeIsProhibited` unexpectedly')

        class OverrideReadOnlyCsv(ReadOnlyCsv):
            """
            if override Csv or ModelCsv, Meta class is reset.
            """

        try:
            OverrideReadOnlyCsv.for_read(
                table=[[i for i in range(3)] for _ in range(5)])
        except OverrideReadOnlyCsv.ReadModeIsProhibited:
            self.fail('`OverrideReadOnlyCsv` raise '
                      '`ReadModeIsProhibited` unexpectedly')

        try:
            OverrideReadOnlyCsv.for_write(instances=[])
        except OverrideReadOnlyCsv.WriteModeIsProhibited:
            self.fail('`OverrideReadOnlyCsv` raise '
                      '`WriteModeIsProhibited` unexpectedly')

        class OverrideWriteOnlyCsv(WriteOnlyCsv):
            pass

        try:
            OverrideWriteOnlyCsv.for_read(
                table=[[i for i in range(3)] for _ in range(5)])
        except OverrideWriteOnlyCsv.ReadModeIsProhibited:
            self.fail('`OverrideWriteOnlyCsv` raise '
                      '`ReadModeIsProhibited` unexpectedly')

        try:
            OverrideWriteOnlyCsv.for_write(instances=[])
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
            ReadAndWriteCsv.for_write(instances=[])
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
                return datetime(2022, 6, 25, tzinfo=timezone.get_current_timezone())

            def column_date_(self, **kwargs):
                return date(2022, 6, 25)

        input_row = ['no', 'yes', '2022-06-25 00:00:00', '2022-06-25']

        boolean_for_read = DefaultConvertCsv.for_read(
            table=[input_row for _ in range(5)])

        expected_result_for_read = {
            'false': False, 'true': True,
            'date_time': datetime(2022, 6, 25, tzinfo=timezone.get_current_timezone()),
            'date_': date(2022, 6, 25),
        }
        self.assertTrue(boolean_for_read.is_valid())
        row = list(boolean_for_read.cleaned_rows)[0]
        self.assertDictEqual(row.values, expected_result_for_read)

        boolean_for_write = DefaultConvertCsv.for_write(instances=[1])
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
        self.assertTrue(boolean_for_read.is_valid())
        row = list(boolean_for_read.cleaned_rows)[0]
        self.assertDictEqual(row.values, expected_result_for_read)

        boolean_for_write = ShowBooleanCsv.for_write(instances=[1])
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
        self.assertTrue(boolean_for_read.is_valid())
        row = list(boolean_for_read.cleaned_rows)[0]
        self.assertDictEqual(row.values, {
            'false': 'False', 'true': 'True', 'date_time': '22/06/25 00:00:00',
            'date_': '22/06/25', 'false2': 'false', 'true2': 'true',
        })

        boolean_for_write = OverrideShowBooleanCsv.for_write(instances=[1])
        row = boolean_for_write.get_table(header=False)[0]
        self.assertListEqual(row, [
            'False', 'True', '2022-06-25 00:00:00+09:00', '2022-06-25', 'False', 'True'
        ])

    def test_insert_blank_column(self):
        class NotPaddingCsv(Csv):
            zero = columns.StaticColumn(static_value=0, index=0)
            one = columns.StaticColumn(static_value=1, index=1)
            three = columns.StaticColumn(static_value=3, index=3)

        for_write = NotPaddingCsv.for_write(instances=[1])
        row = for_write.get_table(header=False)[0]
        self.assertEqual(len(row), 4)
        self.assertListEqual(row, ['0', '1', '', '3'])

        class PaddingCsv(NotPaddingCsv):
            class Meta:
                insert_blank_column = False

        for_write = PaddingCsv.for_write(instances=[1])
        row = for_write.get_table(header=False)[0]
        self.assertEqual(len(row), 3)
        self.assertListEqual(row, ['0', '1', '3'])

    def test_attr_name_and_method_suffix(self):
        @dataclasses.dataclass
        class TestData:
            pk: int
            name: str

        class NotSetAttrName(Csv):
            pk = columns.AttributeColumn()
            name = columns.AttributeColumn()

            class Meta:
                auto_assign = True

            def column_pk(self, instance: TestData, **kwargs) -> str:
                return f'fixed: {instance.pk}'

        data = [TestData(pk=pk, name=f'name {pk}') for pk in range(10)]

        for_write = NotSetAttrName.for_write(instances=data)
        table = for_write.get_table()
        self.assertListEqual(['pk', 'name'], table[0])
        for obj, row in zip(data, table[1:]):
            self.assertListEqual([f'fixed: {obj.pk}', f'name {obj.pk}'], row)

        class SetAttrName(Csv):
            pk = columns.AttributeColumn()
            primary_key = columns.AttributeColumn(attr_name='pk')
            data_name = columns.AttributeColumn(attr_name='name')

            class Meta:
                auto_assign = True

            def column_pk(self, instance: TestData, **kwargs) -> str:
                # this method fix both pk and primary_key values.
                return f'pk: {instance.pk}'

            def column_primary_key(self, instance: TestData, **kwargs) -> str:
                # this method does not called.
                raise ValueError('`column_primary_key` should not be called')

            def column_data_name(self, instance: TestData, **kwargs) -> str:
                raise ValueError('`column_data_name` should not be called')

        for_write = SetAttrName.for_write(instances=data)
        table = for_write.get_table()
        self.assertListEqual(['pk', 'primary_key', 'data_name'], table[0])
        for obj, row in zip(data, table[1:]):
            self.assertListEqual(
                [f'pk: {obj.pk}', f'pk: {obj.pk}', f'name {obj.pk}'],
                row
            )

        class SetMethodSuffix(Csv):
            pk = columns.MethodColumn()
            primary_key = columns.MethodColumn(method_suffix='primary_key')
            data_name = columns.MethodColumn(method_suffix='data_name')

            class Meta:
                auto_assign = True

            def column_pk(self, instance: TestData, **kwargs) -> str:
                # this method fix both pk and primary_key values.
                return f'pk: {instance.pk}'

            def column_primary_key(self, instance: TestData, **kwargs) -> str:
                # this method does not called.
                return f'primary_key: {instance.pk}'

            def column_data_name(self, instance: TestData, **kwargs) -> str:
                return f'data_name: {instance.name}'

        for_write = SetMethodSuffix.for_write(instances=data)
        table = for_write.get_table()
        self.assertListEqual(['pk', 'primary_key', 'data_name'], table[0])
        for obj, row in zip(data, table[1:]):
            self.assertListEqual(
                [f'pk: {obj.pk}', f'primary_key: {obj.pk}', f'data_name: name {obj.pk}'],  # NOQA
                row
            )

    def test_decorator(self):
        @dataclasses.dataclass
        class TestData:
            first: int
            second: str
            third: date
            fourth: bool

        class MethodColumnCsv(Csv):
            first = columns.MethodColumn(header='first', to=int)
            second = columns.MethodColumn(header='second')
            third = columns.MethodColumn(header='third', to=date)
            fourth = columns.MethodColumn(header='fourth', to=bool)

            class Meta:
                auto_assign = True

            def column_first(self, instance: TestData, **kwargs):
                return f'First {instance.first}'

            def column_second(self, instance: TestData, **kwargs):
                return f'Second {instance.second}'

            def column_third(self, instance: TestData, **kwargs):
                return f'Third {instance.third}'

            def column_fourth(self, instance: TestData, **kwargs):
                return f'Fourth {instance.fourth}'

        class DecoratorColumnCsv(Csv):
            PREFIX_FIRST = 'First'
            PREFIX_SECOND = 'Second'
            PREFIX_THIRD = 'Third'
            PREFIX_FOURTH = 'Fourth'

            class Meta:
                auto_assign = True

            @columns.as_column(header='first', to=int)
            def first(self, instance: TestData, **kwargs):
                return f'{self.PREFIX_FIRST} {instance.first}'

            @columns.as_column(header='second')
            def second(self, instance: TestData, **kwargs):
                return f'{self.PREFIX_SECOND} {instance.second}'

            @columns.as_column(header='third', to=date)
            def third(self, instance: TestData, **kwargs):
                return f'{self.PREFIX_THIRD} {instance.third}'

            @columns.as_column(header='fourth', to=bool)
            def fourth(self, instance: TestData, **kwargs):
                return f'{self.PREFIX_FOURTH} {instance.fourth}'

        today = datetime.now().date()
        data = [TestData(first=i,
                         second=f'str {i}', third=today + timedelta(i),
                         fourth=bool(i % 2)) for i in range(10)]

        mth = MethodColumnCsv.for_write(instances=data)
        deco = DecoratorColumnCsv.for_write(instances=data)

        for i, (m_row, d_row) in enumerate(zip(mth.get_table(), deco.get_table())):
            with self.subTest(f'row={i}'):
                self.assertListEqual(m_row, d_row)

        table = [
            [
                str(i), f'str {i}', str(today + timedelta(i)),
                'Yes' if i % 2 else 'No'
            ] for i in range(10)
        ]
        mcsv = DecoratorColumnCsv.for_read(table=table)
        self.assertTrue(mcsv.is_valid())
        for tdata, row in zip(data, mcsv.cleaned_rows):
            with self.subTest():
                self.assertDictEqual(dataclasses.asdict(tdata), row.values)

    def test_convert(self):

        class ConvertCsv(Csv):
            string = columns.AttributeColumn(to=str)
            boolean = columns.AttributeColumn(to=bool)
            integer = columns.AttributeColumn(to=int)
            float_ = columns.AttributeColumn(to=float)
            date_ = columns.AttributeColumn(to=date)
            date_time = columns.AttributeColumn(to=datetime)

            class Meta:
                auto_assign = True
                show_true = 'Yes'
                show_false = 'No'

        @dataclasses.dataclass
        class Data:
            integer: int

            def __post_init__(self):
                self.string = f'string {self.integer}'
                self.boolean = bool(self.integer % 2)
                self.float_ = self.integer + self.integer / 10
                self.date_time = timezone.now() + timedelta(days=self.integer)

                self.date_ = self.date_time.astimezone(
                    ConvertCsv._meta.tzinfo
                ).date()

        data = [Data(integer=i) for i in range(10)]
        mcsv = ConvertCsv.for_write(instances=data)
        now = timezone.now()
        for i, row in enumerate(mcsv.get_table(header=False)):
            with self.subTest(f'row={i}'):
                self.assertEqual(f'string {i}', row[0])
                self.assertEqual('Yes' if i % 2 else 'No', row[1])
                self.assertEqual(str(i), row[2])
                self.assertEqual(str(i + i / 10), row[3])
                dt = (now + timedelta(days=i)).astimezone(mcsv._meta.tzinfo)
                self.assertEqual(
                    dt.date().strftime(mcsv._meta.date_format), row[4])
                self.assertEqual(
                    dt.strftime(mcsv._meta.datetime_format), row[5])

        class ConvertMethodCsv(Csv):
            class Meta:
                auto_assign = True

            @columns.as_column(to=str)
            def integer(self, instance: Data, **kwargs) -> str:
                return str(instance.integer)

            @columns.as_column(to=float)
            def float_(self, instance: Data, **kwargs) -> str:
                return f'{instance.integer}.{instance.integer}'

            @columns.as_column(to=date)
            def date_(self, instance: Data, **kwargs) -> str:
                # to=date but return string.
                # Csv does not raise ValueError and just write down the value.
                return f'Day {instance.integer}'

        mcsv = ConvertMethodCsv.for_write(instances=data)
        try:
            for i, row in enumerate(mcsv.get_table(header=False)):
                self.assertEqual(str(i), row[0])
                self.assertEqual(f'{i}.{i}', row[1])
                self.assertEqual(f'Day {i}', row[2])

        except ValueError:
            self.fail('method return string and ValueError raised unexpectedly')

        # test if ModelCsv cannot convert value to string.
        class ReturnNoneCsv(ConvertCsv):
            class Meta:
                return_none_if_convert_fail = True
                auto_assign = True
                show_true = 'Yes'
                show_false = 'No'

        valid_value = ['string 0', 'Yes', '0', '0.0', '2022-10-22', '2022-10-22 10:00:00']  # NOQA
        # boolean is invalid value.
        invalid_bool = valid_value.copy()
        invalid_bool[1] = 'INVALID BOOL VALUE'
        mcsv = ConvertCsv.for_read(table=[invalid_bool])

        with self.assertRaises(ValueError):
            mcsv.is_valid()

        mcsv = ReturnNoneCsv.for_read(table=[invalid_bool])
        self.assertTrue(mcsv.is_valid())
        try:
            row = list(mcsv.cleaned_rows)[0]
            self.assertIsNone(row['boolean'])
        except ValueError as e:
            self.fail('ValueError raised unexpectedly:' + str(e))

        invalid_integer_and_float = valid_value.copy()
        invalid_integer_and_float[2] = 'INVALID INTEGER'
        invalid_integer_and_float[3] = 'INVALID FLOAT'
        mcsv = ConvertCsv.for_read(table=[invalid_integer_and_float])

        with self.assertRaises(ValueError):
            mcsv.is_valid()

        mcsv = ReturnNoneCsv.for_read(table=[invalid_integer_and_float])
        self.assertTrue(mcsv.is_valid())
        try:
            row = list(mcsv.cleaned_rows)[0]
            self.assertIsNone(row['integer'])
            self.assertIsNone(row['float_'])
        except ValueError as e:
            self.fail('ValueError raised unexpectedly:' + str(e))

        invalid_date_time = valid_value.copy()
        invalid_date_time[4] = 'INVALID DATE'
        invalid_date_time[5] = 'INVALID DATETIME'
        mcsv = ConvertCsv.for_read(table=[invalid_date_time])

        with self.assertRaises(ValueError):
            mcsv.is_valid()

        mcsv = ReturnNoneCsv.for_read(table=[invalid_date_time])
        self.assertTrue(mcsv.is_valid())
        try:
            row = list(mcsv.cleaned_rows)[0]
            self.assertIsNone(row['date_'])
            self.assertIsNone(row['date_time'])
        except ValueError as e:
            self.fail('ValueError raised unexpectedly:' + str(e))

    def test_validation(self):
        class ValidationCsv(Csv):
            string = columns.AttributeColumn(index=0)
            integer = columns.AttributeColumn(index=1, to=int)

            def field_string(self, values: dict, **kwargs) -> str:
                if int(values['string']) % 3 == 0:
                    raise ValidationError('Error')
                return values['string']

            def field_integer(self, values: dict, **kwargs) -> int:
                if values['integer'] % 5 == 0:
                    raise ValidationError('Error')
                return values['integer']

            def field(self, values: dict, **kwargs):
                if values['integer'] > 10:
                    raise ValidationError('Error')

                return values

        mcsv = ValidationCsv.for_read(
            table=[[str(i), str(i)] for i in range(20)]
        )

        with self.assertRaises(AttributeError):
            mcsv.cleaned_rows

        self.assertFalse(mcsv.is_valid())
        self.assertEqual(len(mcsv.cleaned_rows), 20)
        for row in mcsv.cleaned_rows:
            with self.subTest(str(row)):
                expected_is_valid = all([
                    row.number % 3 != 0,
                    row.number % 5 != 0,
                    row.number <= 10
                ])
                self.assertEqual(row.is_valid, expected_is_valid)
                if row.is_valid:
                    self.assertEqual(row.errors, [])
                    self.assertDictEqual(
                        row.values,
                        {'string': str(row.number), 'integer': row.number}
                    )
                    continue

                self.assertEqual(row.values, {})
                error_names = [error.name for error in row.errors]
                if row.number % 3 == 0:
                    self.assertIn('string', error_names)
                    self.assertNotIn('field_method', error_names)

                if row.number % 5 == 0:
                    self.assertIn('integer', error_names)
                    self.assertNotIn('field_method', error_names)

                if all([
                    row.number > 10,
                    row.number % 3 != 0,
                    row.number % 5 != 0,
                ]):
                    # method `field` is not called if `field_` methods raise
                    # ValidationErrors.
                    self.assertIn('field_method', error_names)

                for error in row.errors:
                    self.assertEqual(error.message, 'Error')
