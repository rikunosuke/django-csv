from typing import Union, Type

from . import writers, columns, readers

# Reader Class hints
ExcelReader = Union[readers.XlsReader, readers.XlsxReader]
ExcelReaderClass = Union[Type[readers.XlsReader], Type[readers.XlsxReader]]

# Write Class hints
ExcelWriter = Union[writers.XlsxWriter, writers.XlsxWriter]
ExcelWriterClass = Union[Type[writers.XlsxWriter], Type[writers.XlsxWriter]]

ForeignColumn = Union[columns.BaseForeignColumn]
ForeignColumnClass = Union[Type[columns.BaseForeignColumn]]
