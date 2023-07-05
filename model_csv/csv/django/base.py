import itertools
from typing import Generator, Union, Callable

from django.db import models

from ..base import TableForRead, RowForWrite, BaseModelRowForRead, \
    PartForReadMixin, BasePartMixin


class DjangoRowForRead(BaseModelRowForRead):
    def remove_extra_values(self, values: dict) -> dict:
        """
        remove values which is not in fields
        """
        fields = self._meta.model._meta.get_fields()

        return {
            k: v for k, v in values.items()
            if k in [f.name for f in fields] or k.endswith('_id')
        }


class DjangoCsvForRead(DjangoRowForRead, TableForRead):
    def get_instances(self, only_valid: bool = False) -> Generator:
        if not self.is_valid() and not only_valid:
            raise ValueError('`is_valid()` method failed')

        for row in self.cleaned_rows:
            if not row.is_valid:
                continue

            values = self.remove_extra_values(row.values)
            yield self._meta.model(**values)

    def bulk_create(self, batch_size=100, only_valid: bool = False) -> None:
        iterator = self.get_instances(only_valid=only_valid)

        while True:
            created = list(itertools.islice(iterator, batch_size))
            if not created:
                break

            self._meta.model.objects.bulk_create(created)


class DjangoPartForRead(PartForReadMixin, DjangoRowForRead):

    def get_or_create_object(self, values: dict, **kwargs) -> models.Model:
        values = self.remove_extra_values(values)
        return self.model.objects.get_or_create(**values)[0]

    def create_object(self, values: dict, **kwargs) -> models.Model:
        values = self.remove_extra_values(values)
        return self.model.objects.create(**values)

    def get_object(self, values: dict, **kwargs) -> models.Model:
        values = self.remove_extra_values(values)
        return self.model.objects.get(**values)


class DjangoPartForWrite(RowForWrite):
    def get_row_value(self, instance, is_relation: bool = False
                      ) -> dict[int, str]:
        # get foreign model.
        relation_instance = getattr(instance, self.related_name)
        if not isinstance(relation_instance, self.model):
            raise ValueError(f'Wrong field name. `{self.related_name}` is not '
                             f'{self.model.__class__.__name__}.')
        return super().get_row_value(relation_instance, is_relation=is_relation)


class DjangoBasePart(BasePartMixin, DjangoPartForWrite, DjangoPartForRead):
    def __init__(self, related_name: str,
                 callback: Union[str, Callable] = 'get_or_create_object',
                 **kwargs):
        if self._meta.model is None:
            mcsv_class_name = self.__class__.__name__.split('Part', 1)[0]
            raise ValueError(
                f'django model is not defined in meta class of {mcsv_class_name}'
            )

        self.model = self._meta.model
        super().__init__(related_name, callback, **kwargs)
