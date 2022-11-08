from typing import Optional, Type

from . import hints, writers, columns, readers

from .csv import ModelCsv, Csv  # NOQA: F401


__all__ = [
    'hints', 'writers', 'columns', 'readers', 'ModelCsv', 'Csv'
]


def get_reader_class(filename: str = Optional[None],
                     extenstion: str = Optional[None]) -> Type[readers.Reader]:
    if not extenstion:
        extenstion = filename.split('.')[-1]

    if extenstion == 'csv':
        return readers.CsvReader

    elif extenstion == 'tsv':
        return readers.TsvReader

    elif extenstion == 'xlsx':
        return readers.XlsxReader

    elif extenstion == 'xls':
        return readers.XlsReader

    raise ValueError(f'`{extenstion} is not supported`')


def get_writer_class(filename: str = Optional[None],
                     extenstion: str = Optional[None]) -> Type[writers.Writer]:
    if not extenstion:
        extenstion = filename.split('.')[-1]

    if extenstion == 'csv':
        return writers.CsvWriter

    elif extenstion == 'tsv':
        return writers.TsvWriter

    elif extenstion == 'xlsx':
        return writers.XlsxWriter

    elif extenstion == 'xls':
        return writers.XlsWriter

    raise ValueError(f'`{extenstion} is not supported`')


class FieldFormatError(Exception):
    pass


class ColumnFormatError(Exception):
    pass
