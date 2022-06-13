import csv
import io

import urllib


class Writer:
    content_type = 'text/csv'
    expansion = '.csv'

    def __init__(self, filename: str, encoding: str = 'utf-8') -> None:
        self.filename = filename if filename.endswith(
            self.expansion) else filename + self.expansion
        if len(self.filename.split('.')) != 2:
            raise ValueError(f'{filename} may have unexpected expansion. '
                             f'`{self.expansion}` is expected.')
        self.encoding = encoding

    def make_response(self, table: list, **kwargs) -> 'HttpResponse':
        return self._response()

    def _response(self, **kwargs) -> 'HttpResponse':
        from django.http import HttpResponse
        res = HttpResponse(content_type=self.content_type)
        filename = urllib.parse.quote(self.filename)
        res['Content-Disposition'] = f'attachment;filename="{filename}"'
        return res


class CsvMixin:
    def make_response(self, table: list, **kwargs):
        with io.StringIO() as sio:
            w = csv.writer(sio, delimiter=self.delimiter)
            w.writerows(table)
            res = self._response()
            res.write(sio.getvalue().encode(self.encoding, errors='ignore'))

        return res


class CsvWriter(CsvMixin, Writer):
    delimiter = ','
    expansion = '.csv'
    content_type = 'text/csv'


class TsvWriter(CsvMixin, Writer):
    delimiter = '\t'
    expansion = '.tsv'
    content_type = 'text/tsv'


class ExcelMixin:
    content_type = 'application/vnd.ms-excel'

    def __init__(self, sheet_name: str = 'sheet 1', **kwargs):
        self.sheet_name = sheet_name
        super().__init__(**kwargs)


class XlsWriter(ExcelMixin, Writer):
    expansion = '.xls'

    def make_response(self, table: list, **kwargs):
        import xlwt
        wb = xlwt.Workbook()
        ws = wb.add_sheet(self.sheet_name)
        self.write_down(ws, table)
        res = self._response()
        wb.save(res)
        return res

    @staticmethod
    def write_down(work_sheet: 'WorkSheet', table: list) -> None:
        """
        write down to work sheet.
        """
        for y, row in enumerate(table):
            for x, col in enumerate(row):
                work_sheet.write(y, x, col)


class XlsxWriter(ExcelMixin, Writer):
    expansion = '.xlsx'

    def make_response(self, table: list, **kwargs):
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = self.sheet_name
        self.write_down(ws, table)
        res = self._response()
        wb.save(res)
        return res

    @staticmethod
    def write_down(work_sheet: 'WorkSheet', table: list) -> None:
        for y, row in enumerate(table, 1):
            for x, col in enumerate(row, 1):
                work_sheet.cell(y, x).value = col
