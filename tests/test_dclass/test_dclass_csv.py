import dataclasses
from datetime import datetime, date, timezone
from unittest import TestCase

from django_csv.model_csv.csv.dclass import DataClassCsv


@dataclasses.dataclass
class TestDataclass:
    string: str
    number: int
    boolean: bool
    date_time: datetime
    date_: date


class TestCsv(DataClassCsv):
    class Meta:
        dclass = TestDataclass
        fields = "__all__"


class DataClassCsvTest(TestCase):
    def test_columns(self):
        columns = TestCsv._meta.get_columns()
        self.assertEqual(len(columns), 5)

        self.assertEqual(columns[0].name, "string")
        self.assertEqual(columns[0].to, str)

        self.assertEqual(columns[1].name, "number")
        self.assertEqual(columns[1].to, int)

        self.assertEqual(columns[2].name, "boolean")
        self.assertEqual(columns[2].to, bool)

        self.assertEqual(columns[3].name, "date_time")
        self.assertEqual(columns[3].to, datetime)

        self.assertEqual(columns[4].name, "date_")
        self.assertEqual(columns[4].to, date)

    def test_write(self):
        data = [
            TestDataclass(
                string=f"string{i}", number=i, boolean=bool(i % 2),
                date_time=datetime(2023, 1, 1 + i, i + 1, tzinfo=timezone.utc),
                date_=date(2023, 1, 1 + i)
            ) for i in range(10)
        ]

        mcsv = TestCsv.for_write(instances=data)
        header, *body = mcsv.get_table(header=True)
        self.assertListEqual(
            header,
            ["string", "number", "boolean", "date_time", "date_"]
        )

        for i, row in enumerate(body):
            self.assertListEqual(
                row,
                [
                    f"string{i}",
                    f"{i}",
                    TestCsv._meta.show_true if bool(i % 2) else TestCsv._meta.show_false,
                    datetime(2023, 1, 1 + i, i + 1).strftime(TestCsv._meta.datetime_format),
                    date(2023, 1, 1 + i).strftime(TestCsv._meta.date_format)
                ]
            )
