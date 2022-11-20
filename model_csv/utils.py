from typing import Optional, Type

from . import readers, writers


def get_reader_class(filename: str = Optional[None],
                     extension: str = Optional[None]) -> Type[readers.Reader]:
    if not extension:
        extension = filename.split('.')[-1]

    if extension == 'csv':
        return readers.CsvReader

    elif extension == 'tsv':
        return readers.TsvReader

    elif extension == 'xlsx':
        return readers.XlsxReader

    elif extension == 'xls':
        return readers.XlsReader

    raise ValueError(f'`{extension} is not supported`')


def get_writer_class(filename: str = Optional[None],
                     extension: str = Optional[None]) -> Type[writers.Writer]:
    if not extension:
        extension = filename.split('.')[-1]

    if extension == 'csv':
        return writers.CsvWriter

    elif extension == 'tsv':
        return writers.TsvWriter

    elif extension == 'xlsx':
        return writers.XlsxWriter

    elif extension == 'xls':
        return writers.XlsWriter

    raise ValueError(f'`{extension} is not supported`')
