from typing import Union

from . import writers, columns, readers

# Reader Class hints
ExcelReader = Union[readers.XlsReader, readers.XlsxReader]

# Write Class hints
ExcelWriter = Union[writers.XlsxWriter, writers.XlsxWriter]

ForeignColumn = columns.BaseForeignColumn
