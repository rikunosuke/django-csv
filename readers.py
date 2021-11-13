import csv
import io


class Reader:
    pass


class CSVReader(Reader):
    delimiter = ','

    @classmethod
    def convert_2d_list(cls, *, file, encoding, table_start_from,
                        reader_kwargs: dict = {}) -> list:
        csv_file = io.TextIOWrapper(
            file.file, encoding=encoding, **reader_kwargs
        )
        return list(csv.reader(csv_file, delimiter=cls.delimiter)
                    )[table_start_from:]


class TSVReader(CSVReader):
    delimiter = '\t'


class XLSReader(Reader):
    @classmethod
    def convert_2d_list(cls, *, file, encoding, table_start_from,
                        reader_kwargs: dict = {}) -> list:
        import xlrd

        wb = xlrd.open_workbook(file_contents=file.read(), **reader_kwargs)
        sheet = wb.sheets()[0]
        return [['' if value is None else value
                 for value in sheet.row_values(n)]
                for n in range(table_start_from, sheet.nrows)]


class XLSXReader(Reader):
    @classmethod
    def convert_2d_list(cls, *, file, encoding, table_start_from,
                        **kwargs) -> list:
        from openpyxl import load_workbook

        wb = load_workbook(file, data_only=True)
        sheet = wb.worksheets[0]
        table = [['' if cell.value is None else cell.value for cell in row]
                 for row in sheet.iter_rows(min_row=table_start_from + 1)]
        return table


class EXCELReader(Reader):
    @classmethod
    def convert_2d_list(cls, *, file, **kwargs):
        if file.name.endswith('.xls'):
            return XLSReader.convert_2d_list(file=file, **kwargs)
        return XLSXReader.convert_2d_list(file=file, **kwargs)
