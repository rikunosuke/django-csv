from django_csv.mcsv.base import BaseCsv, CsvForRead, ModelCsvForRead, BasePart
from django_csv.mcsv.metaclasses import CsvMetaclass, ModelCsvMetaclass


class PartCreateMixin:
    @classmethod
    def as_part(cls, field_name: str,
                callback: str = 'get_or_create_object') -> BasePart:

        class MetaForPart:
            as_part = True

        return type(f'{cls.__name__}Part', (BasePart, cls), {'Meta': MetaForPart})(
            field_name=field_name,  callback=callback)


class Csv(PartCreateMixin, BaseCsv, metaclass=CsvMetaclass):
    read_class = CsvForRead


class ModelCsv(PartCreateMixin, BaseCsv, metaclass=ModelCsvMetaclass):
    read_class = ModelCsvForRead
