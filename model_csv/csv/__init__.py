from .base import BaseCsv, CsvForRead, ModelCsvForRead, BasePart, \
    ValidationError
from .metaclasses import CsvMetaclass, ModelCsvMetaclass


class Csv(BaseCsv, metaclass=CsvMetaclass):
    read_class = CsvForRead


class ModelCsv(BaseCsv, metaclass=ModelCsvMetaclass):
    read_class = ModelCsvForRead

    @classmethod
    def as_part(cls, related_name: str,
                callback: str = 'get_or_create_object') -> BasePart:

        return type(
            f'{cls.__name__}Part', (cls, BasePart),
            {'_meta': cls._meta}
        )(related_name=related_name, callback=callback)
