from django.contrib import admin
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse

from django_csv.model_csv.csv.django import DjangoCsv
from django_csv.model_csv.writers import CsvWriter, TsvWriter, XlsWriter, XlsxWriter

from .forms import UploadForm


class DjangoCsvAdminMixin:
    actions = ["download_csv", "download_tsv", "download_xlsx", "download_xls"]
    file_name: str = "CsvFile"
    csv_class: DjangoCsv = None
    csv_upload_form = UploadForm
    error_message = "Error"

    @admin.action(description="download (.csv)")
    def download_csv(self, request, queryset):
        mcsv = self.csv_class.for_write(instances=queryset)
        return mcsv.get_response(CsvWriter(filename=f"{self.file_name}.csv"))

    @admin.action(description="download (.tsv)")
    def download_tsv(self, request, queryset):
        mcsv = self.csv_class.for_write(instances=queryset)
        return mcsv.get_response(TsvWriter(filename=f"{self.file_name}.tsv"))

    @admin.action(description="download (.xlsx)")
    def download_xlsx(self, request, queryset):
        mcsv = self.csv_class.for_write(instances=queryset)
        return mcsv.get_response(XlsxWriter(filename=f"{self.file_name}.xlsx"))

    @admin.action(description="download (.xls)")
    def download_xls(self, request, queryset):
        mcsv = self.csv_class.for_write(instances=queryset)
        return mcsv.get_response(XlsWriter(filename=f"{self.file_name}.xls"))

    def get_urlname(self, suffix: str) -> str:
        return f"{self.model._meta.app_label}_{self.model._meta.model_name}_{suffix}"

    def get_urls(self):
        urls = super().get_urls()

        new_url = [
            path(
                "upload/",
                self.admin_site.admin_view(self.upload_csv),
                name=self.get_urlname("upload_csv"),
            ),
        ]
        return new_url + urls

    def upload_csv(self, request):
        if request.method == "GET":
            return self.get_response(request, form=self.csv_upload_form())

        form = self.csv_upload_form(request.POST, request.FILES)
        if not form.is_valid():
            return self.get_response(request, form=form)

        READER = form.cleaned_data["reader"]
        reader = READER(file=form.cleaned_data["file"])

        headers, *table = reader.get_table()
        expected_headers = self.csv_class._meta.get_headers(for_read=True)
        if headers != expected_headers:
            self.message_user(
                request,
                f"Column order must be {expected_headers}. Not {headers}",
                level="ERROR",
            )
            return self.get_response(request, form=form)

        mcsv = self.csv_class.for_read(table=table)
        mcsv.set_static("only_exists", form.cleaned_data["only_exists"])
        if mcsv.is_valid():
            mcsv.bulk_create()
            return redirect(reverse(f'admin:{self.get_urlname("changelist")}'))

        self.message_user(request, self.error_message, level="ERROR")
        return TemplateResponse(
            request,
            "admin/django_csv/upload_csv.html",
            {"form": form, "rows": mcsv.cleaned_rows},
        )

    def get_response(self, request, **kwargs):
        return TemplateResponse(request, "admin/django_csv/upload_csv.html", kwargs)
