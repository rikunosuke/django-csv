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
            ' '.join([extension.upper() for extension in READER.keys()]) +
            ' are available.'
        )
    )
    only_exists = forms.BooleanField(
        required=False, initial=True,
        help_text='raise Exception if a publisher does not exist'
    )

    def clean(self):
        cleaned_data = super().clean()
        file = cleaned_data['file']
        extension = file.name.split('.')[-1]

        try:
            cleaned_data['reader'] = READER[extension.lower()]
        except KeyError:
            raise forms.ValidationError(f'`{extension}` is not supported')

        return cleaned_data
