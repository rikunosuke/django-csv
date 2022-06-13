from unittest import TestCase

from django.db import models

from mcsv import columns
from mcsv.mcsv import Csv, ModelCsv


class CsvTest(TestCase):
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
