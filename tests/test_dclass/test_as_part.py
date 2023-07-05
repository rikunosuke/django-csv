import dataclasses
from pathlib import Path
from unittest import TestCase

from django_csv.model_csv.csv.dclass import DataClassCsv
from django_csv.model_csv.readers import CsvReader


@dataclasses.dataclass(slots=True)
class Publisher:
    name: str
    headquarter: str


@dataclasses.dataclass(slots=True)
class Book:
    title: str
    price: int
    publisher: Publisher
    is_on_sale: bool
    description: str

    def __str__(self) -> str:
        return self.title

    @property
    def name(self) -> str:
        return f'{self.title}  ({self.publisher.name})'


class PublisherCsv(DataClassCsv):
    class Meta:
        dclass = Publisher
        fields = "__all__"

    def column_country(self, instance: Publisher, **kwargs) -> str:
        return instance.headquarter.split(',')[1].strip()

    def column_city(self, instance: Publisher, **kwargs) -> str:
        return instance.headquarter.split(',')[0].strip()

    def field_headquarter(self, values: dict, **kwargs) -> str:
        city = values['city'].strip()
        country = values['country'].strip()
        return city + ', ' + country


class BookWithPublisherCsv(DataClassCsv):
    pbl = PublisherCsv.as_part(related_name="publisher")

    pbl_name = pbl.AttributeColumn(header="Publisher", attr_name="name")
    pbl_country = pbl.MethodColumn(header="Country", method_suffix="country", value_name="country")
    pbl_city = pbl.MethodColumn(header="City", method_suffix="city", value_name="city")

    class Meta:
        dclass = Book
        fields = "__all__"
        auto_assign = True

        headers = {
            "is_on_sale": "is on sale",
        }


class PartTest(TestCase):
    def test_headers(self):
        self.assertListEqual(
            BookWithPublisherCsv._meta.get_headers(for_read=True),
            [
                "title",
                "price",
                "is on sale",
                "description",
                "Publisher",
                "Country",
                "City",
            ]
        )

    def test_dataclass_for_read(self):
        with (Path(__file__).parent / "test_data" / "book.csv").open() as f:
            reader = CsvReader(file=f)
            mcsv = BookWithPublisherCsv.for_read(
                table=reader.get_table(table_starts_from=1)
            )
        if not mcsv.is_valid():
            self.fail(f"Validation Error: {mcsv.errors}")
        instances = list(mcsv.get_instances())
        self.assertEqual(len(instances), 50)

        for i, instance in enumerate(instances, 1):
            with self.subTest(f"row = {i}"):
                self.assertIsInstance(instance, Book)
                self.assertIsInstance(instance.publisher, Publisher)

    def test_dataclass_for_write(self):
        publishers = [
            Publisher(
                name=f"Publisher {i}",
                headquarter=f"City {i}, Country {i}",
            ) for i in range(10)
        ]

        books = [
            Book(
                title=f"Book {i}",
                price=i * 100,
                publisher=publishers[i % 10],
                is_on_sale=i % 2 == 0,
                description=f"Description {i}",
            ) for i in range(50)
        ]

        mcsv = BookWithPublisherCsv.for_write(instances=books)
        body = mcsv.get_table(header=False)

        for i, row in enumerate(body):
            with self.subTest(f"row = {i}"):
                self.assertListEqual(
                    row,
                    [
                        f"Book {i}",
                        str(i * 100),
                        "yes" if i % 2 == 0 else "no",
                        f"Description {i}",
                        f"Publisher {i % 10}",
                        f"Country {i % 10}",
                        f"City {i % 10}",
                    ]
                )
