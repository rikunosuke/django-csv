import csv
import io
from django.http import HttpResponse

import urllib


class Writer:
    @classmethod
    def make_response(cls, filename, table: list, **kwargs):
        return HttpResponse()

    @classmethod
    def _response(cls, filename, content_type='text/csv', **kwargs):
        res = HttpResponse(content_type=content_type)
        filename = urllib.parse.quote(filename)
        res['Content-Disposition'] = f'attachment;filename="{filename}"'
        return res


class CSVWriter(Writer):
    delimiter = ','
    expansion = '.csv'

    @classmethod
    def make_response(cls, filename, table: list, **kwargs):
        sio = io.StringIO()
        w = csv.writer(sio, delimiter=cls.delimiter)
        w.writerows(table)
        res = cls._response(
            filename=f'{filename}{cls.expansion}', content_type='text/tsv')
        res.write(sio.getvalue().encode('cp932', errors='ignore'))
        return res


class TSVWriter(CSVWriter):
    delimiter = '\t'
    expansion = '.tsv'


class XLSWriter(Writer):
    @classmethod
    def make_response(cls, filename, table: list, **kwargs):
        import xlwt
        wb = xlwt.Workbook()
        ws = wb.add_sheet('sheet 1')
        for y, row in enumerate(table):
            for x, col in enumerate(row):
                ws.write(y, x, col)
        res = cls._response(filename=f'{filename}.xls',
                            content_type='application/vnd.ms-excel')
        wb.save(res)
        return res


class XLSXWriter(Writer):
    @classmethod
    def make_response(cls, filename, table: list, **kwargs):
        from openpyxl import Workbook
        wb = Workbook()
        sheet = wb.active
        sheet.title = 'sheet 1'
        for y, row in enumerate(table, 1):
            for x, col in enumerate(row, 1):
                sheet.cell(y, x).value = col
        res = cls._response(filename=f'{filename}.xlsx',
                            content_type='application/vnd.ms-excel')
        wb.save(res)
        return res
