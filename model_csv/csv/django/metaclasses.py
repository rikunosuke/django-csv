from datetime import datetime, date
from typing import Optional, Dict, List

from django.db import models
from django.utils import timezone

from ..metaclasses import CsvOptions, BaseMetaclass
from ...columns import BaseColumn, AttributeColumn


def get_type_from_model_field(field: models.Field):
    if isinstance(field, models.IntegerField):
        return int

    if isinstance(field, models.FloatField):
        return float

    if isinstance(field, models.BooleanField):
        return bool

    if isinstance(field, models.DateTimeField):
        return datetime

    if isinstance(field, models.DateField):
        return date

    return str


class DjangoOptions(CsvOptions):
    tzinfo = timezone.get_current_timezone()
    headers: Dict[str, str]

    ALLOWED_META_ATTR = CsvOptions.ALLOWED_META_ATTR + (
        'model',
        'fields',
        'headers',
        'as_part',
    )

    def __init__(self, meta: Optional[type], columns: Dict[str, BaseColumn],
                 parts: List['Part']):
        if meta is None:
            raise ValueError('class `Meta` is required in `DjangoCsv`')

        if not hasattr(meta, 'model'):
            raise ValueError('`model` is required in class `Meta.`')

        self.model = meta.model

        if getattr(meta, 'as_part', False):
            # DjangoCsvOptions of Part Class only has own relation columns.
            super().__init__(meta, {}, parts)
            return

        if not (field_names := getattr(meta, 'fields', None)):
            super().__init__(meta, columns, parts)
            return

        # auto create AttributeColumns for fields.
        if field_names == '__all__':
            fields = [f for f in self.model._meta.get_fields()
                      if not f.auto_created and not f.is_relation]
        else:
            fields = [self.model._meta.get_field(name) for name in field_names]

        # skip if the name is already used.
        column_names = columns.keys()
        fields = [f for f in fields if f.name not in column_names]

        _kwargs = {
            'columns': list(columns.values()),
            'original': True
        }

        unassigned_r = self.get_unassigned(
            [
                col.get_r_index(original=True)
                for col in self.filter_columns(r_index=True, **_kwargs)
            ]
        )

        unassigned_w = self.get_unassigned(
            [
                col.get_w_index(original=True)
                for col in self.filter_columns(w_index=True, **_kwargs)
            ]
        )

        for r, w, f in zip(unassigned_r, unassigned_w, fields):
            header = meta.headers.get(f.name) if hasattr(meta,
                                                         'headers') else None
            if not header:
                header = getattr(f, 'verbose_name', f.name)

            to = get_type_from_model_field(f)

            columns.update({
                f.name: AttributeColumn(
                    r_index=r, w_index=w, header=header, to=to
                )
            })

        super().__init__(meta, columns, parts)


class DjangoCsvMetaclass(BaseMetaclass):
    option_class = DjangoOptions
