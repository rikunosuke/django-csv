from datetime import datetime
from typing import Optional, Dict, Iterable, List, Any

import copy

from mcsv.columns import BaseColumn, AttributeColumn


class Options:

    read_mode: bool = True
    write_mode: bool = True
    datetime_format: str = '%Y-%m-%d %H:%M:%S'
    show_true: str = 'yes'
    show_false: str = 'no'
    as_true: Iterable = ['yes', 'Yes']
    as_false: Iterable = ['no', 'No']

    ALLOWED_META_ATTR_NAMES = (
        'read_mode',
        'write_mode',
        'datetime_format',
        'show_true',
        'show_false',
        'as_true',
        'as_false',
        'auto_assign',
    )

    APPLY_ONLY_MAIN_META_ATTR_NAMES = tuple()

    class UnknownAttribute(Exception):
        pass

    class UnknownColumn(Exception):
        pass

    def __init__(self, meta: type, *args, **kwargs):
        meta = copy.deepcopy(meta)

        # validate meta attrs and set attr to Options.
        for attr_name in dir(meta):
            if attr_name.startswith('_'):
                continue

            if attr_name not in self.ALLOWED_META_ATTR_NAMES:
                raise self.UnknownAttribute(
                    f'Unknown Attribute is defined. `{attr_name}`')

            val = getattr(meta, attr_name)
            if attr_name in ('as_true', 'as_false'):
                if isinstance(val, str):
                    raise TypeError(
                        f'`{attr_name}` must be list, tuple or set.')

            setattr(self, attr_name, val)

    def convert_from_str(self, value: str) -> Any:
        if value in self.as_true:
            return True

        elif value in self.as_false:
            return False

        try:
            return datetime.strptime(value, self.datetime_format)
        except ValueError:
            return value

    def convert_to_str(self, value: Any) -> str:
        if type(value) == datetime:
            return value.strftime(self._meta.datetime_format)

        elif type(value) == bool:
            return self._meta.show_true if value else self._meta.show_false

        else:
            return str(value)


class PartOptions(Options):
    ALLOWED_META_ATTR_NAMES = Options.ALLOWED_META_ATTR_NAMES + (
        'model',
    )


class CsvOptions(Options):
    def __init__(self, meta, columns: Dict[str, BaseColumn],
                 *args, **kwargs):
        super().__init__(meta, *args, **kwargs)
        self.columns = []
        for name, column in columns.copy().items():
            if column.is_relation:
                column.name = column.get_name(name)
            else:
                column.name = column.attr_name or name
            self.columns.append(column)

        if getattr(self, 'auto_assign', False):
            self.assign_number()

    @staticmethod
    def filter_columns(*, columns: List[BaseColumn], r_index: bool = None,
                       w_index: bool = None, for_write: bool = None,
                       for_read: bool = None, read_value: bool = None,
                       write_value: bool = None, is_static: bool = None,
                       original: bool = False) -> list:
        """
        filter passed columns and return them if they match conditions.
        """
        other_params = not all(map(
            lambda x: x is None,
            [r_index, is_static, read_value, write_value]))

        if for_read is not None:
            if other_params or original:
                raise TypeError('`for_write` with other params not supported')

            if for_read:
                columns = [
                    col for col in columns
                    if col.is_static or col.r_index is not None
                ]
                read_value = True
            else:
                columns = [
                    col for col in columns
                    if not col.read_value or
                    (not col.is_static and col.r_index is None)
                ]

        if for_write is not None:
            if other_params or original:
                raise TypeError('`for_write` with other params not supported')

            if for_write:
                w_index = True
                write_value = True
            else:
                columns = [
                    col for col in columns
                    if col.w_index is None or not col.write_value
                ]

        if r_index is not None:
            columns = [col for col in columns if any([
                isinstance(col.get_r_index(original), int) and r_index,
                col.get_r_index(original) is None and not r_index,
            ])]

        if w_index is not None:
            columns = [col for col in columns if any([
                isinstance(col.get_w_index(original), int) and w_index,
                col.get_w_index(original) is None and not w_index,
            ])]

        if read_value is not None:
            columns = [col for col in columns if col.read_value == read_value]

        if write_value is not None:
            columns = [col for col in columns if col.write_value == write_value]

        if is_static is not None:
            columns = [col for col in columns if col.is_static == is_static]

        return columns

    @staticmethod
    def get_unassigned(assigned: list) -> Iterable:
        """
        return the list of index which is not assigned yet.
        """
        i = 0
        while True:
            if i not in assigned:
                yield i

            i += 1

    def get_columns(self, *, r_index: bool = None, w_index: bool = None,
                    for_write: bool = None, for_read: bool = None,
                    read_value: bool = None, write_value: bool = None,
                    is_static: bool = None, original: bool = False,):

        return self.filter_columns(
            r_index=r_index, w_index=w_index, for_read=for_read,
            for_write=for_write, read_value=read_value, write_value=write_value,
            is_static=is_static, original=original, columns=self.columns)

    def get_column(self, name: str) -> BaseColumn:
        for col in self.columns:
            if col.name == name:
                return col
        else:
            raise self.UnknownColumn(f'UnknownColumn `{name}`')

    def assign_number(self) -> None:
        """
        Set r and w indexes to the columns not having original indexes.
        If run, all indexes automatically assigned are overwritten.
        """
        for i, col in zip(self.get_unassigned(self.get_r_indexes(True)),
                          self.get_columns(
                              r_index=False, original=True, read_value=True)):
            col.r_index = i

        for i, col in zip(self.get_unassigned(self.get_w_indexes(True)),
                          self.get_columns(
                              w_index=False, original=True, write_value=True)):
            col.w_index = i

    def get_r_indexes(self, original: bool = False) -> list:

        return [col.get_r_index(original) for col in self.get_columns(
            r_index=True, original=original)]

    def get_w_indexes(self, original: bool = False) -> list:

        return [col.get_w_index(original) for col in self.get_columns(
            w_index=True, original=original)]


class ModelOptions(CsvOptions):
    APPLY_ONLY_MAIN_META_ATTR_NAMES = Options.APPLY_ONLY_MAIN_META_ATTR_NAMES + (
        'model',
        'fields',
    )

    ALLOWED_META_ATTR_NAMES = Options.ALLOWED_META_ATTR_NAMES + (
        'model',
        'fields'
    )

    def __init__(self, meta: Optional[type], columns: Dict[str, BaseColumn]):
        model = getattr(meta, 'model', None)
        if not model:
            super().__init__(meta, columns)
            return

        field_names = getattr(meta, 'fields')

        if field_names == '__all__':
            fields = [f for f in model._meta.get_fields()
                      if not f.auto_created and not f.is_relation]
        else:
            fields = [model._meta.get_field(name) for name in field_names]

        unassigned_r = self.get_unassigned([
            col.get_r_index(original=True) for col in self.filter_columns(
                r_index=True, original=True, columns=list(columns.values()))
        ])
        unassigned_w = self.get_unassigned([
            col.get_w_index(original=True) for col in self.filter_columns(
                w_index=True, original=True, columns=list(columns.values()))
        ])

        column_names = columns.keys()
        for r, w, f in zip(unassigned_r, unassigned_w, fields):
            if f.name in column_names:
                continue

            header = getattr(f, 'verbose_name', f.name)
            columns.update({
                f.name: AttributeColumn(r_index=r, w_index=w, header=header)
            })

        super().__init__(meta, columns)


class BaseMetaclass(type):
    """
    create Options class from Meta and add _meta to attrs.
    """

    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        if not any([isinstance(b, mcs) for b in bases]):
            return super().__new__(mcs, name, bases, attrs)

        # meta クラスを対応
        attrs['_meta'] = mcs.option_class(
            meta=mcs.__get_meta(attrs.get('Meta'), bases),
            columns=dict(
                mcs.__concat_columns(bases=bases, attrs=attrs).items()
            )
        )

        return super().__new__(mcs, name, bases, attrs)

    @classmethod
    def __concat_columns(mcs, bases: tuple, attrs: dict) -> dict:
        col_dict = {}
        for attr_name, attr in attrs.items():
            if isinstance(attr, BaseColumn):
                setattr(attr, 'name', attr_name)
                col_dict.update({attr_name: attr})

        for base in bases:
            if hasattr(base, '_meta'):
                col_dict.update({
                    col.name: col for col in base._meta.get_columns()})

        return col_dict

    @classmethod
    def __get_meta(mcs, meta: Optional[type], bases: tuple) -> type:
        metas = list(
            copy.deepcopy(getattr(base, 'Meta'))
            for base in bases if hasattr(base, 'Meta')
        )

        for meta in metas:
            for attr in mcs.option_class.APPLY_ONLY_MAIN_META_ATTR_NAMES:
                if hasattr(meta, attr):
                    delattr(meta, attr)
        if meta:
            metas.append(meta)

        return type('Meta', tuple(set(metas)), {})


class PartMetaclass(BaseMetaclass):
    option_class = PartOptions


class CsvMetaclass(BaseMetaclass):
    option_class = CsvOptions


class ModelCsvMetaclass(BaseMetaclass):
    option_class = ModelOptions
