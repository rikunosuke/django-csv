import dataclasses
from typing import Generator, Callable, Union

from ..base import BaseModelRowForRead, TableForRead, PartForReadMixin, \
    RowForWrite, BasePartMixin


class DataClassRowForRead(BaseModelRowForRead):
    def remove_extra_values(self, values: dict) -> dict:
        return {
            k: v for k, v in values.items()
            if k in [f.name for f in dataclasses.fields(self._meta.dclass)]
        }


class DataClassCsvForRead(DataClassRowForRead, TableForRead):
    def get_instances(self, only_valid: bool = False) -> Generator:
        if not self.is_valid() and not only_valid:
            raise ValueError('`is_valid()` method failed')

        for row in self.cleaned_rows:
            if not row.is_valid:
                continue

            values = self.remove_extra_values(row.values)
            yield self._meta.dclass(**values)


class DataClassPartForRead(PartForReadMixin, DataClassRowForRead):
    def get_dataclass(self, values: dict, **kwargs) -> dataclasses.dataclass:
        return self.dclass(**self.remove_extra_values(values))


class DataClassPartForWrite(RowForWrite):
    def get_row_value(self, instance, is_relation: bool = False) -> dict[int, str]:
        # get foreign model.
        relation_instance = getattr(instance, self.related_name)
        if not isinstance(relation_instance, self.dclass):
            raise ValueError(f'Wrong field name. `{self.related_name}` is not '
                             f'{self.dclass.__class__.__name__}.')
        return super().get_row_value(relation_instance,
                                     is_relation=is_relation)


class DataClassBasePart(BasePartMixin, DataClassPartForWrite, DataClassPartForRead):
    def __init__(self, related_name: str, callback: Union[str, Callable] = "get_dataclass", **kwargs):
        if self._meta.dclass is None:
            mcsv_class_name = self.__class__.__name__.split('Part', 1)[0]
            raise ValueError(
                f'django model is not defined in meta class of {mcsv_class_name}'
            )

        self.dclass = self._meta.dclass
        super().__init__(related_name, callback, **kwargs)
