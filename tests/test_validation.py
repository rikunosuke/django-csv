from django.test import TestCase

from django_csv.model_csv import Csv, ValidationError, columns


class ValidationTest(TestCase):
    def test_capture_validation_error(self):
        class TestException(Exception):
            pass

        class ExceptionCsv(Csv):
            string = columns.AttributeColumn(index=0)

            def field_string(self, values: dict, **kwargs):
                raise TestException()

        csv = ExceptionCsv.for_read(table=[[""]])
        with self.assertRaises(TestException):
            csv.is_valid()

        class ValidationExceptionCsv(Csv):
            string = columns.AttributeColumn(index=0)

            def field_string(self, values: dict, **kwargs):
                raise ValidationError()

        csv = ValidationExceptionCsv.for_read(table=[[""]])
        try:
            self.assertFalse(csv.is_valid())
        except ValidationError:
            self.fail("Validation Error raised unexpectedly")

    def test_row_class(self):
        error_message = "string must not be blank"

        class RowClassCsv(Csv):
            string = columns.AttributeColumn(index=0)

            def field_string(self, values: dict, **kwargs):
                string: str = values["string"]
                if not string:
                    raise ValidationError(error_message, label="string")
                return string

        # invalid
        csv = RowClassCsv.for_read(table=[[""]])
        self.assertFalse(csv.is_valid())
        rows = csv.cleaned_rows
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0].errors), 1)
        self.assertEqual(rows[0].number, 0)
        error = rows[0].errors[0]
        self.assertEqual(error.message, error_message)
        self.assertEqual(error.label, "string")
        self.assertEqual(error.column_index, 0)

        # valid
        csv = RowClassCsv.for_read(table=[["string"]])
        self.assertTrue(csv.is_valid())
        rows = csv.cleaned_rows
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0].errors), 0)
        row = rows[0]
        self.assertDictEqual(row.values, {"string": "string"})
