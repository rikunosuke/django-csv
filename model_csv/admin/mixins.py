from django.contrib import admin
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import reverse, path

from ..csv import ModelCsv
from ..writers import TsvWriter, CsvWriter, XlsxWriter, XlsWriter
from .forms import UploadForm


class ModelCsvAdminMixin:
    actions = ['download_csv', 'download_tsv', 'download_xlsx', 'download_xls']
    file_name: str = 'CsvFile'
    csv_class: ModelCsv = None

    @admin.action(description='download (.csv)')
    def download_csv(self, request, queryset):
        mcsv = self.csv_class.for_write(instances=queryset)
        return mcsv.get_response(CsvWriter(filename=f'{self.file_name}.csv'))

    @admin.action(description='download (.tsv)')
    def download_tsv(self, request, queryset):
        mcsv = self.csv_class.for_write(instances=queryset)
        return mcsv.get_response(TsvWriter(filename=f'{self.file_name}.tsv'))

    @admin.action(description='download (.xlsx)')
    def download_xlsx(self, request, queryset):
        mcsv = self.csv_class.for_write(instances=queryset)
        return mcsv.get_response(XlsxWriter(filename=f'{self.file_name}.xlsx'))

    @admin.action(description='download (.xls)')
    def download_xls(self, request, queryset):
        mcsv = self.csv_class.for_write(instances=queryset)
        return mcsv.get_response(XlsWriter(filename=f'{self.file_name}.xls'))

    def get_urlname(self, suffix: str) -> str:
        return f'{self.model._meta.app_label}_{self.model._meta.model_name}_{suffix}'

    def get_urls(self):
        urls = super().get_urls()

        new_url = [
            path('upload/', self.admin_site.admin_view(self.upload_csv),
                 name=self.get_urlname('upload_csv')),
        ]
        return new_url + urls

    def upload_csv(self, request):
        if request.method == 'GET':
            return TemplateResponse(
                request,
                'admin/django_csv/upload_csv.html', {'form': UploadForm()})
        else:
            form = UploadForm(request.POST, request.FILES)
            if not form.is_valid():
                return TemplateResponse(
                    request,
                    'admin/django_csv/upload_csv.html', {'form': form})

            READER = form.cleaned_data['reader']
            reader = READER(file=form.cleaned_data['file'], table_starts_from=1)

            mcsv = self.csv_class.for_read(table=reader.get_table())
            mcsv.bulk_create()

            return redirect(reverse(f'admin:{self.get_urlname("changelist")}'))
