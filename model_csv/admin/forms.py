from django import forms

from .. import readers

READER = {
    'csv': readers.CsvReader,
    'tsv': readers.TsvReader,
    'xlsx': readers.XlsxReader,
    'xls': readers.XlsReader,
}


class UploadForm(forms.Form):
    file = forms.FileField(
        required=True,
        help_text=(
            ' '.join([extenstion.upper() for extenstion in READER.keys()]) +
            ' are available.'
        )
    )

    def clean(self):
        cleaned_data = super().clean()
        file = cleaned_data['file']
        extenstion = file.name.split('.')[-1]

        try:
            cleaned_data['reader'] = READER[extenstion.lower()]
        except KeyError:
            raise forms.ValidationError(f'`{extenstion}` is not supported')

        return cleaned_data
