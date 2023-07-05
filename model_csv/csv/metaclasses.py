from collections import OrderedDict
from datetime import datetime, date, timezone
from typing import Dict, Iterable, List, Any

import copy

from .base import BasePartMixin
from ..columns import BaseColumn
from ..utils import render_row


class CsvOptions:
    read_mode: bool = True
    write_mode: bool = True
    datetime_format: str = '%Y-%m-%d %H:%M:%S'
    date_format: str = '%Y-%m-%d'
    tzinfo = timezone.utc
    show_true: str = 'yes'
    show_false: str = 'no'
    as_true: Iterable = ['yes', 'Yes']
    as_false: Iterable = ['no', 'No']
    auto_convert = True
    return_none_if_convert_fail = False
    auto_assign = False
    insert_blank_column: bool = True
    default_if_none = ''

    ALLOWED_META_ATTR = (
        'read_mode',
        'write_mode',
        'datetime_format',
        'date_format',
        'tzinfo',
        'show_true',
        'show_false',
        'as_true',
        'as_false',
        'auto_convert',
        'return_none_if_convert_fail',
        'auto_assign',
        'insert_blank_column',
        'default_if_none',
    )

    class UnknownAttribute(Exception):
        pass

    class UnknownColumn(Exception):
        pass

    def __init__(self, meta, columns: Dict[str, BaseColumn],
                 parts: List[BasePartMixin]):

        self.parts = parts
        meta = copy.deepcopy(meta)

        # validate meta attrs and set attr to Options.
        for attr_name in dir(meta):
            if attr_name.startswith('_'):
                continue

            if attr_name not in self.ALLOWED_META_ATTR:
                raise self.UnknownAttribute(
                    f'Unknown Attribute is defined. `{attr_name}`')

            val = getattr(meta, attr_name)
            # easy validation for `as_true` and `as_false`
            if attr_name in ('as_true', 'as_false'):
                if isinstance(val, str):
                    raise TypeError(
                        f'`{attr_name}` must be list, tuple or set.')

            setattr(self, attr_name, val)

        self.columns = []
        for name, column in columns.copy().items():
            column.name = name
            self.columns.append(column)

        if self.auto_assign:
            self.assign_number()

    def convert_from_str(self, value: Any, to: Any, column_index: int) -> Any:
        if not self.auto_convert or not isinstance(value, str):
            return value

        if to == str:
            return value

        if to == int:
            if value in (self.default_if_none, ''):
                return None
            try:
                return int(value)
            except ValueError:
                if self.return_none_if_convert_fail:
                    return None
                else:
                    raise

        if to == float:
            if value in (self.default_if_none, ''):
                return None
            try:
                return float(value)
            except ValueError:
                if self.return_none_if_convert_fail:
                    return None
                else:
                    raise

        if to == bool:
            if value in self.as_true:
                return True

            elif value in self.as_false:
                return False
            else:
                if self.return_none_if_convert_fail:
                    return None
                else:
                    raise ValueError(
                        f'`{value}` is not in both `as_true` and `as_false`')

        if to in (date, datetime):
            try:
                naive = datetime.strptime(value, self.datetime_format)
            except (ValueError, TypeError):
                pass
            else:
                if self.tzinfo:
                    if naive.tzinfo is None:
                        return naive.replace(tzinfo=self.tzinfo)
                return naive

            try:
                return datetime.strptime(value, self.date_format).date()
            except ValueError:
                if self.return_none_if_convert_fail:
                    return None
                else:
                    raise

        return value

    def convert_to_str(self, value: Any, to: Any) -> str:
        if not self.auto_convert or to == str or isinstance(value, str):
            return str(value)

        if value is None:
            return self.default_if_none

        if to == bool:
            return self.show_true if value else self.show_false

        if to == datetime:
            if not isinstance(value, datetime):
                raise ValueError(f'`{value}` is not a datetime instance')

            if self.tzinfo:
                value = value.astimezone(self.tzinfo)

            return value.strftime(self.datetime_format)

        elif to == date:
            if not isinstance(value, date):
                raise ValueError(f'`{value}` is not a date instance')
            return value.strftime(self.date_format)

        return str(value)

    @staticmethod
    def filter_columns(*, columns: List[BaseColumn], r_index: bool = None,
                       w_index: bool = None, for_write: bool = None,
                       for_read: bool = None, read_value: bool = None,
                       write_value: bool = None, is_static: bool = None,
                       is_relation: bool = None, original: bool = False
                       ) -> list:
        """
        filter passed columns and return them if they match conditions.
        r_index: if True, return columns having `r_index.`
        for_read: if True, return columns which are for read.
        read_value: if True, return columns whose `read_value` is True.
        is_static: if True, return columns which are static columns.
        is_relation: if True, return columns which are columns for a relation model.
        original: use this param with `r_index` or `w_index.` if true, return
                  only columns which have original r or w indexes.
                  original index is index defined by user, not automatically assigned.
        """
        if is_relation is not None:
            columns = [col for col in columns if col.is_relation == is_relation]

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
        return a generator of index which is not assigned yet.
        """
        i = 0
        while True:
            if i not in assigned:
                yield i

            i += 1

    def get_columns(self, *, r_index: bool = None, w_index: bool = None,
                    for_write: bool = None, for_read: bool = None,
                    read_value: bool = None, write_value: bool = None,
                    is_static: bool = None, is_relation: bool = None,
                    original: bool = False):

        return self.filter_columns(
            r_index=r_index, w_index=w_index, for_read=for_read,
            for_write=for_write, read_value=read_value, write_value=write_value,
            is_static=is_static, is_relation=is_relation, original=original,
            columns=self.columns)

    def get_column(self, name: str) -> BaseColumn:
        for col in self.columns:
            if col.name == name:
                return col
        else:
            raise self.UnknownColumn(f'UnknownColumn `{name}`')

    def get_header(self, name: str) -> str:
        return self.get_column(name).header

    def get_headers(self, for_read: bool = False, for_write: bool = False
                    ) -> list[str]:
        if for_read == for_write:
            raise ValueError('choose read mode or write mode')

        attr_names = 'get_r_index' if for_read else 'get_w_index'
        cols = {
            getattr(col, attr_names)(): col.header
            for col in self.get_columns(
                for_read=for_read or None, for_write=for_write or None
            )
        }
        return render_row(cols, insert_blank_column=self.insert_blank_column)

    def assign_number(self) -> None:
        """
        Set r and w indexes to the columns not having original indexes.
        If this method calls, all indexes assigned automatically are overwritten.
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


class BaseMetaclass(type):
    """
    create Options class from Meta and add _meta to attrs.
    """

    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        if not any([isinstance(b, mcs) for b in bases]):
            return super().__new__(mcs, name, bases, attrs)

        # meta クラスを対応
        if '_meta' not in attrs:
            attrs['_meta'] = mcs.option_class(
                meta=attrs.get('Meta'),
                columns=mcs.__concat_columns(bases=bases, attrs=attrs),
                parts=mcs.__concat_parts(bases=bases, attrs=attrs),
            )

        return super().__new__(mcs, name, bases, attrs)

    @classmethod
    def __concat_columns(mcs, bases: tuple, attrs: dict) -> dict:
        col_dict = OrderedDict(
            (attr_name, attr) for attr_name, attr in attrs.items()
            if isinstance(attr, BaseColumn)
        )

        for base in reversed(bases):
            if hasattr(base, '_meta'):
                col_dict.update({
                    col.name: col for col in base._meta.get_columns()})

        return col_dict

    @classmethod
    def __concat_parts(mcs, bases: tuple, attrs: dict) -> list:
        parts = [
            attr for attr in attrs.values() if isinstance(attr, BasePartMixin)
        ]

        for base in bases:
            if hasattr(base, '_meta'):
                parts.extend([part for part in base._meta.parts])

        return parts


class CsvMetaclass(BaseMetaclass):
    option_class = CsvOptions
