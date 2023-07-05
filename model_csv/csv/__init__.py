from .base import BaseCsv, CsvForRead
from .metaclasses import CsvMetaclass


class Csv(BaseCsv, metaclass=CsvMetaclass):
    read_class = CsvForRead
