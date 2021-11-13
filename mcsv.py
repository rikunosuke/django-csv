import inspect
import io
from functools import cached_property
from typing import Generator, Any, List, Dict

from . import readers, writers


class NoHeaderError(Exception):
    pass


class NoSuchFieldError(Exception):
    pass


class CsvColumn:
    is_static = None

    def __init__(self, *, header: str = None, index: int = None,
                 no_header_error: bool = False, r_index: int = None,
                 w_index: int = None, csv_output: bool = True,
                 model_field: bool = True):
        """
        header: CSV の列名 読み込み時は列の順番を特定するために使われる
        index: CSVの列の順番 0 から始まる
        r_index: 読み込み時に参照する列の順番
        w_index: 書き込みされる列の順番
        no_header_error: 読み込み時、 header で列の順番を特定できなかった場合に
                         エラーを出す場合は True
        csv_output: CSV に出力する場合は True
        model_field: Model に値を渡す場合は True
        """

        self.r_index = index if r_index is None else r_index
        self.w_index = index if w_index is None else w_index
        self.header = header
        self.no_header_error = no_header_error
        self.csv_output = csv_output
        self.model_field = model_field

    def validate_for_read(self) -> None:
        if self.is_static:
            return

        if self.header is None or self.r_index is None:
            raise ValueError('header か r_index どちらかの値が必用です')

        if not self.no_header_error and self.r_index is None:
            raise ValueError('no_header_error を False にする場合は '
                             'index または r_indexを指定してください')

    def validate_for_write(self) -> None:
        if not self.csv_output:
            return

        if self.w_index is None:
            raise ValueError('w_index または index を設定するか、'
                             'csv_output を False にしてください')

    def get_value_for_read(self, row: List[str], **kwargs):
        """
        CSV の値を読み込む際に呼び出される
        row: CSV の行をリスト化したもの
        """
        return row[self.r_index]

    def get_value_for_write(self, instance, field_name: str, **kwargs) -> Any:
        """
        CSV に値を書き込む際に呼び出される
        instance: models.Model のインスタンス
        field_name: models.Model のフィールド名
        """
        value = instance
        for field in field_name.split('__'):
            try:
                value = getattr(value, field)
            except AttributeError:
                raise NoSuchFieldError(
                    f'"{value.__class__.__name__}"には"{field}"がありません')
        return value

    def get_index_for_write(self) -> int:
        """
        CSV に書き込むための列の順番を返す
        """
        if self.w_index is None:
            raise ValueError('w_index または index を指定してください')

        return self.w_index


class DynamicColumn(CsvColumn):
    """
    CSV とモデルを動的に関連付ける
    """
    is_static = False

    def get_value_for_read(self, row: List[str], header: List[str] = None):
        """
        CSV の値を読み込む際に呼び出される
        row: CSV の行をリスト化したもの
        header: 列名のリスト
        """
        index = self.r_index
        if self.header is not None and header:
            index = self._get_index_from_header(header)

        return row[index]

    def _get_index_from_header(self, header: List[str]) -> int:
        """
        対象の列の順番を列名から特定して返す
        列から特定できなかった場合は r_index を返す
        header: 列名のリスト
        """
        try:
            return header.index(self.header)
        except ValueError:
            if self.no_header_error:
                raise NoHeaderError('ヘッダーが見つかりませんでした')
            return self.r_index


class StaticColumn(CsvColumn):
    """
    CSV とモデルを静的に関連づける
    CSV に依らない静的な値をモデルに渡したり、
    モデルに依らない値を CSVに書き込む際に使う
    出力ID などを想定
    """
    is_static = True

    def __init__(self, *, static_value: Any = '', **kwargs):
        """
        static_value: 静的な値 主に書き込み時に使われる
        """
        self.static_value = static_value
        super().__init__(**kwargs)

    def get_value_for_read(self, *args, **kwargs) -> Any:
        return self.static_value

    def get_value_for_write(self, *args, **kwargs) -> Any:
        return self.static_value


class ModelCsvBase:
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


class RIndexOverColumnNumberError(Exception):
    pass


class ModelCsvForRead(ModelCsvBase):
    """
    読み込み用のクラス
    .for_read() で作成する
    Model のフィールドに渡す値を動的に作成する場合は
    field_*フィールド名* のメソッドを定義する。
    """

    def __init__(self, *, file: io.BytesIO, has_header: bool, encoding: str,
                 table_start_from: int, reader_class: reader.Reader,
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

    def get_csv_header(self) -> List[str] or None:
        """
        ヘッダー部分をリストで返す
        """
        return self.table[0] if self.has_header else None

    def get_csv_data(self) -> List[List[str]] or None:
        """
        CSV のヘッダーを除いたデータ部分を2次元のリストにして返す
        """
        start = 1 if self.has_header else 0
        return self.table[start:]

    def get_instances(self) -> Generator:
        """
        CSV から作成した Model のインスタンスを返すジェネレーター
        """
        csv_data = self.get_csv_data()
        max_r_index = max([col.r_index for col in self._columns.values()])
        if len(csv_data[1]) < max_r_index:
            raise RIndexOverColumnNumberError(
                f'column number: {len(csv_data[1])} < r_index: {max_r_index}')
        for row in csv_data:
            yield self.model(**self._get_model_values(row))

    def bulk_create(self, batch_size=100) -> None:
        """
        CSV から作成した Model を DB に保存する
        batch_size: 一度にジェネレーターから作成するインスタンスの最大値
        """
        models = []
        for instance in self.get_instances():
            models.append(instance)
            if len(models) >= batch_size:
                self.model.objects.bulk_create(models)
                models = []

        if models:
            self.model.objects.bulk_create(models)

    def _add_dynamic_fields(self, values: dict) -> dict:
        """
        動的なフィールドの値を作成する
        values: 各 CsvColumn から取り出した {field 名: 値} の辞書型
        def field_*(self, values: dict) -> Any:
        * 部分をフィールド名にする。
        必用ないフィールドのデータは _remove_extra_values で消す
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        mark = 'field_'
        updated = values.copy()
        for name, mthd in methods:
            if name.startswith(mark):
                # 特定の値が消されないように .copy() をする
                value = mthd(values=values.copy(), static=self._static.copy())
                field_name = name.split(mark)[1]
                updated[field_name] = value
        return updated

    def _remove_extra_values(self, values: dict) -> dict:
        """
        Model にないフィールドの値を取り除く
        """
        cleaned_values = {}
        for key, value in values.items():
            if key in self.existing_fields:
                cleaned_values.update({key: value})
        return cleaned_values

    def _get_model_values(self, row: List[str]) -> Dict[str, Any]:
        """
        CSV から値を取り出して {field 名: 値} の辞書型を渡す
        CsvColumn インスタンスが model_field=False の場合は含めない
        """
        values = {}
        header = self.get_csv_header()
        for name, column in self._columns.items():
            if not column.model_field:
                continue
            values.update({
                name: column.get_value_for_read(row=row, header=header)
            })

        # CsvColumn にない値を動的に作成
        values = self._add_dynamic_fields(values)
        values = self._remove_extra_values(values)
        return values

    def _run_column_validation(self):
        cols = [getattr(self, name) for name in self._columns]
        for col in cols:
            if col:
                col.validate_for_read()
        index = [col.r_index for col in cols if col.r_index is not None]
        if len(index) != len(set(index)):
            raise ValueError('CsvColumn の index に重複があります')

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
        return [field.name for field in self.model._meta.fields]


class ModelCsvForWrite(ModelCsvBase):
    """
    書き込み用のクラス
    .for_wtite() で作成する
    列の値に編集を加える場合は
    column_*列名* のメソッドを定義する
    blank_column: w_index の値が連続していない場合、True で空欄の列を挿入する
    """
    blank_column = False

    def __init__(self, queryset):
        self.queryset = queryset

    def get_response(self, filename: str,
                     writer_class: writer.Writer = writer.CSVWriter,
                     header: bool = False, **kwargs):

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
            for name, col in self._columns_for_write.items():
                values = self._get_row_value(query)
                updated = self._update_values(
                    instance=query, values=values)
                rows.update(
                    {col.get_index_for_write(): updated.get(name, '')})

            table.append(self._render_row(rows))

        return table

    def _get_header(self) -> List[str]:
        """
        ヘッダーの情報を {列の順番: 列名} の辞書型で返す
        CsvColumn に header が定義されていない場合は CsvColumn
        のクラス変数名を列名にする
        """
        headers = {}
        for name, column in self._columns_for_write.items():
            headers.update({
                column.get_index_for_write():
                    getattr(column, 'header', name)})

        return self._render_row(headers)

    def _get_row_value(self, instance) -> Dict[str, str]:
        """
        行の値を {列名: 値} で返す
        """
        row = {}
        for name, column in self._columns_for_write.items():
            row.update({
                name: column.get_value_for_write(instance, name)
            })
        return row

    def _update_values(self, instance,
                       values: Dict[str, str]) -> Dict[str, str]:
        """
        CsvColumn .get_value_for_write() で取得した値を書き換える
        def column_*(self, values: dict) -> dict:
        で動的に値を作成する
        """
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        mark = 'column_'
        for name, mthd in methods:
            if name.startswith(mark):
                column_name = name.split(mark)[1]
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
        cols = [getattr(self, name) for name in self._columns]
        for col in cols:
            if col:
                col.validate_for_write()

        index = [col.w_index for col in cols if col.csv_output]
        if len(index) != len(set(index)):
            raise ValueError('CsvColumn の index に重複があります')

    @cached_property
    def _columns_for_write(self):
        """
        csv_output = True の CsvColumn を返す
        """
        columns = {}
        for name, column in self._columns.items():
            if not column.csv_output:
                continue
            columns.update({name: column})
        return columns


class NotAllowedReadType(Exception):
    pass


class NotAllowedWriteType(Exception):
    pass


class ModelCsvMetaClass(type):
    """
    CsvColumn を {クラス変数名: CsvColumn インスタンス}
    の辞書型で _columns に格納するメタクラス
    """
    def __new__(mcs, name, bases, attrs):
        columns = {}

        for attr_name, attr in attrs.items():
            if isinstance(attr, CsvColumn):
                columns.update({attr_name: attr})
        for base in [base for base in bases if hasattr(base, '_columns')]:
            columns.update({key: val for key, val in base._columns.items()})
        attrs['_columns'] = columns

        return super().__new__(mcs, name, bases, attrs)


class ModelCsv(metaclass=ModelCsvMetaClass):
    read_type = True
    write_type = True

    def __init__(self, *args, **kwargs):
        self._run_column_validation()
        super().__init__(*args, **kwargs)

    @classmethod
    def for_read(cls, file: io.BytesIO, has_header: bool,
                 encoding: str = 'cp932', table_start_from: int = 0,
                 reader_class: reader.Reader = reader.CSVReader,
                 reader_kwargs: dict = {}):
        """
        CSV 読み込みようのインスタンスを返す
        """

        if not cls.read_type:
            raise NotAllowedReadType('Not Allowed read type.')

        class TempModelCSVForRead(cls, ModelCsvForRead):
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
            raise NotAllowedWriteType('Not Allowed write type')

        class TempModelCSVForWrite(cls, ModelCsvForWrite):
            pass

        return TempModelCSVForWrite(queryset=queryset)
