import inspect
import io
from functools import cached_property
from typing import Generator, Any, List, Dict, Optional

from . import readers, writers
from .columns import BaseColumn


class CsvBase:
    model = None
    _static = {}

    def set_static_column(self, column_name: str, value: Any) -> None:
        """
        StaticColumn の static_value を書き換える
        """
        column = getattr(self, column_name, None)
        if column and column.is_static:
            column.static_value = value

    def set_static(self, key, value) -> None:
        self._static.update({key: value})


class CsvForRead(CsvBase):
    """
    読み込み用のクラス
    .for_read() で作成する
    Model のフィールドに渡す値を動的に作成する場合は
    field_*フィールド名* のメソッドを定義する。
    """

    class RIndexOverColumnNumberError(Exception):
        pass

    def __init__(self, *, file: io.BytesIO, has_header: bool, encoding: str,
                 table_start_from: int, reader_class: readers.Reader,
                 reader_kwargs: dict) -> None:
        """
        file: request.FILES[file_name].file で取得できる
        has_header: CSV にヘッダーがついている場合は True
        encodig: file をエンコーディングする際の文字コード名
        table_start_from: テーブルが始まる行
        reader_class: file を2次元配列に変換する Reader モデル
        reader_kwargs: Reader モデルに渡す引数
        static: field_* で使う定数を格納する
        """
        self.has_header = has_header
        self.file = file
        self.encoding = encoding
        self.table_start_from = table_start_from
        self.reader_class = reader_class
        self.rkws = reader_kwargs

    def get_csv_header(self) -> Optional[List[str]]:
        """
        ヘッダー部分をリストで返す
        """
        return self.table[0] if self.has_header else None

    def get_csv_data(self) -> Optional[List[str]]:
        """
        CSV のヘッダーを除いたデータ部分を2次元のリストにして返す
        """
        start = 1 if self.has_header else 0
        return self.table[start:]

    def _add_dynamic_fields(self, values: dict) -> dict:
        """
        動的なフィールドの値を作成する
        values: 各 Column から取り出した {field 名: 値} の辞書型
        def field_*(self, values: dict) -> Any:
        * 部分をフィールド名にする。
        必用ないフィールドのデータは _remove_extra_values で消す
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        prefix = 'field_'
        updated = values.copy()
        for name, mthd in methods:
            if not name.startswith(prefix):
                continue

            field_name = name.split(prefix)[1]
            # 特定の値が消されないように .copy() をする
            updated[field_name] = mthd(
                values=values.copy(), static=self._static.copy(),
            )
        return updated

    def get_values(self, row: List[str]) -> Dict[str, Any]:
        """
        CSV から値を取り出して {field 名: 値} の辞書型を渡す
        Column インスタンスが model_field=False の場合は含めない
        """
        values = {}
        for name, column in self._r_columns.items():
            values.update({
                name: column.get_value_for_read(row=row, fieldname=name)})

        # Column にない値を動的に作成
        return self._add_dynamic_fields(values)

    @cached_property
    def table(self) -> List[str]:
        """
        CSV の値を2次元のリストで返す
        """
        return self.reader_class.convert_2d_list(
            file=self.file, encoding=self.encoding,
            table_start_from=self.table_start_from, reader_kwargs=self.rkws)


class ModelCsvForRead(CsvForRead):
    def get_values(self, row: List[str]) -> Dict[str, Any]:
        values = super().get_values(row)
        values = self._make_relations(values)
        values = self._remove_extra_values(values)
        return values

    def get_instances(self) -> Generator:
        """
        CSV から作成した Model のインスタンスを返すジェネレーター
        """
        csv_data = self.get_csv_data()
        max_r_index = max([col.r_index for col in self._columns.values()])
        if len(csv_data[1]) < max_r_index:
            raise CsvForRead.RIndexOverColumnNumberError(
                f'column number: {len(csv_data[1])} < r_index: {max_r_index}')
        for row in csv_data:
            yield self.model(**self.get_values(row))

    def bulk_create(self, batch_size=100) -> None:
        """
        CSV から作成した Model を DB に保存する
        batch_size: 一度にジェネレーターから作成するインスタンスの最大値
        """
        models = []
        instances = list(self.get_instances())
        length = len(instances)
        for i, instance in enumerate(instances, 1):
            models.append(instance)
            if len(models) % batch_size == 0 or i == length:
                self.model.objects.bulk_create(models)
                models = []

    def _remove_extra_values(self, values: dict) -> dict:
        """
        Model にないフィールドの値を取り除く
        """
        cleaned_values = {}
        for key, value in values.items():
            if key in self.existing_fields:
                cleaned_values.update({key: value})
        return cleaned_values

    def _make_relations(self, values: dict) -> dict:
        for cols in self._relations:
            values.update({cols.fieldname: cols.call_relations()})
        return values

    def _run_column_validation(self):
        for col in self._columns.values():
            col.validate_for_read()
        index = [col.r_index for col in self._r_columns.values()]
        if len(index) != len(set(index)):
            raise ValueError(
                '`index` must be unique. Change `index` or `r_index`')

    @cached_property
    def table(self) -> List[str]:
        """
        CSV の値を2次元のリストで返す
        """
        return self.reader_class.convert_2d_list(
            file=self.file, encoding=self.encoding,
            table_start_from=self.table_start_from, reader_kwargs=self.rkws)

    @cached_property
    def existing_fields(self) -> list:
        """
        Model の存在するフィールド名のリスト
        """
        return [field.name for field in self.model._meta.get_fields()]


class CsvForWrite(CsvBase):
    """
    書き込み用のクラス
    .for_write() で作成する
    列の値に編集を加える場合は
    column_*列名* のメソッドを定義する
    blank_column: w_index の値が連続していない場合、True で空欄の列を挿入する
    """
    blank_column = False

    def __init__(self, queryset):
        self.queryset = queryset

    def get_response(self, filename: str,
                     writer_class: writers.Writer = writers.CSV,
                     header: bool = True, **kwargs):

        return writer_class.make_response(
            filename, self.get_table_as_list(header=header), **kwargs)

    def get_table_as_list(self, header: bool = True):
        """
        ヘッダー情報と Model インスタンスから作成した値を2次元のリストで返す
        """
        table = []
        if header:
            table.append(self._get_header())
        for query in self.queryset:
            # self._render_row に渡すために
            # {index: value} の辞書型をつくる
            rows = {}
            for name, col in self._w_columns.items():
                values = self._get_row_value(query)
                updated = self._update_values(
                    instance=query, values=values)
                rows.update(
                    {col.w_index: updated.get(name, '')})

            table.append(self._render_row(rows))

        return table

    def _get_header(self) -> List[str]:
        """
        ヘッダーの情報を {列の順番: 列名} の辞書型で返す
        Column に header が定義されていない場合は Column
        のクラス変数名を列名にする
        """
        headers = {}
        for name, column in self._w_columns.items():
            headers.update({
                column.w_index:
                    getattr(column, 'header', name)})

        return self._render_row(headers)

    def _get_row_value(self, instance) -> Dict[str, str]:
        """
        行の値を {列名: 値} で返す
        """
        row = {}
        for name, column in self._w_columns.items():
            row.update({
                name: column.get_value_for_write(instance, name)
            })
        return row

    def _update_values(self, instance,
                       values: Dict[str, str]) -> Dict[str, str]:
        """
        Column .get_value_for_write() で取得した値を書き換える
        def column_*(self, values: dict) -> dict:
        で動的に値を作成する
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        prefix = 'column_'
        for name, mthd in methods:
            if name.startswith(prefix):
                column_name = name.split(prefix)[1]
                column_value = mthd(
                    instance=instance, values=values.copy(),
                    static=self._static.copy())
                values[column_name] = column_value

        return values

    def _render_row(self, maps: Dict[int, Any]) -> List[str]:
        """
        列の順番 通りに並び替えた 値 のリストを返す
        maps: {列の順番: 値} の辞書型
        """
        rows = []
        for i in range(max(list(maps.keys())) + 1):
            try:
                value = maps[i]
                rows.append(value)
            except KeyError:
                if self.blank_column:
                    rows.append('')

        return rows

    def _run_column_validation(self):
        for col in self._columns.values():
            col.validate_for_write()
        index = [col.w_index for col in self._w_columns.values()]
        if len(index) != len(set(index)):
            raise ValueError(
                '`index` must be unique. Change `index` or `w_index`')


class CsvMetaClass(type):
    """
    Column を {クラス変数名: Column インスタンス}
    の辞書型で _columns に格納するメタクラス
    """

    def __new__(mcs, name, bases, attrs):
        columns = {}

        for attr_name, attr in attrs.items():
            if isinstance(attr, BaseColumn):
                columns.update({attr_name: attr})
        for base in [base for base in bases if hasattr(base, '_columns')]:
            columns.update({key: val for key, val in base._columns.items()})
        attrs['_columns'] = columns
        attrs['_w_columns'] = {k: v for k, v in columns.items() if
                               v.csv_output}
        attrs['_r_columns'] = {k: v for k, v in columns.items() if
                               v.model_field}
        attrs['_relations'] = set(
            [v._main for v in columns.values() if v.is_relation])

        return super().__new__(mcs, name, bases, attrs)


class BaseCsvClass(metaclass=CsvMetaClass):
    read_type = True
    write_type = True

    read_class = None
    write_class = None

    class NotAllowedReadType(Exception):
        pass

    class NotAllowedWriteType(Exception):
        pass

    def __init__(self, *args, **kwargs):
        self._run_column_validation()
        super().__init__(*args, **kwargs)

    @classmethod
    def for_read(cls, file: io.BytesIO, has_header: bool = True,
                 encoding: str = 'cp932', table_start_from: int = 0,
                 reader_class: readers.Reader = readers.CSVReader,
                 reader_kwargs: dict = {}):
        """
        CSV 読み込みようのインスタンスを返す
        """

        if not cls.read_type:
            raise cls.NotAllowedReadType('Not Allowed read type.')

        class TempModelCSVForRead(cls, cls.read_class):
            pass

        return TempModelCSVForRead(
            file=file, has_header=has_header, encoding=encoding,
            table_start_from=table_start_from, reader_class=reader_class,
            reader_kwargs=reader_kwargs)

    @classmethod
    def for_write(cls, queryset):
        """
        CSV 書き込みようのインスタンスを返す
        """
        if not cls.write_type:
            raise cls.NotAllowedWriteType('Not Allowed write type')

        class TempModelCSVForWrite(cls, cls.write_class):
            pass

        return TempModelCSVForWrite(queryset=queryset)


class CsvClass(BaseCsvClass):
    read_class = CsvForRead
    write_class = CsvForWrite


class ModelCsv(BaseCsvClass):
    read_class = ModelCsvForRead
    write_class = CsvForWrite
