from typing import Optional, Type, Any

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


def render_row(maps: dict[int, Any], insert_blank_column: bool) -> list[str]:
    """
    convert maps from dict to list ordered by index.
    maps: {index: value}
    """
    def __get_value_from_map(_i) -> Optional[str]:
        try:
            return maps[_i]
        except KeyError:
            if insert_blank_column:
                return ''

    return list(filter(
        lambda x: x is not None,
        [
            __get_value_from_map(i)
            for i in range(max(list(maps.keys())) + 1)
         ]
    ))
