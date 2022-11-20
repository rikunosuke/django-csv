from typing import Union, Type

from . import writers, columns, readers

# Reader Class hints
ExcelReader = Union[readers.XlsReader, readers.XlsxReader]

# Write Class hints
ExcelWriter = Union[writers.XlsxWriter, writers.XlsxWriter]

ForeignColumn = columns.BaseForeignColumn
