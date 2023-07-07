# model-csv
model-csv makes it easy to treat csv, tsv and excel file.

# How to use model-csv
## 1. Create ModelCsv classes.
### Django model
```python3
class Book(models.Model):
    title = models.CharField(max_length=100)
    price = models.PositiveIntegerField(null=True, blank=True)
    is_on_sale = models.BooleanField(default=True)
    description = models.TextField()

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


from model_csv.csv.django import DjangoCsv


class BookCsv(DjangoCsv):
    class Meta:
        model = Book
        # if you declare fields then ModelCsv create columns automatically.
        fields = '__all__'
```

### dataclasses.dataclass

```python3
@dataclasses.dataclass
class Book:
    title: str
    price: int
    is_on_sale: bool
    description: str


from model_csv.csv.dclass import DataClassCsv


class BookCsv(DataClassCsv):
    class Meta:
        dclass = Book
        fields = '__all__'
```

## 2.1 Create a 2d list of instances if you want to create csv file.
```python3
>>> mcsv = BookCsv.for_write(instances=book_instances)
>>> mcsv.get_table(header=True)
[['title', 'price', 'is_on_sale', 'description'], ['Book title', '540', 'YES', 'description'], ...]

# if django model
# choose file type. (CsvWriter, TsvWriter, XlsxWriter)
>>> from model_csv.writers import CsvWriter
>>> writer = CsvWriter(file_name='book')
# only DjangoCsv classes can make a django response.
>>> mcsv.get_response(writer=writer)
```

### Django - Download and Upload View Example.
```python3
# Download csv file in django view.
class BookDownloadView(generic.View):
    def get(self, **kwargs):
        mcsv = BookCsv.for_write(instance=Book.objects.all())
        writer = CsvWriter(file_name='book.csv')
        return mcsv.get_response(writer=writer)

# Upload csv file in django view.
class BookUploadView(generic.View):
    def form_valid(self, form, **kwargs):
        file = form.cleaned_data['file']
        reader = CsvReader(file=file, table_starts_from=1)
        headers, *table = reader.get_table()
        if headers != BookCsv._meta.get_headers():
            form.add_error('file', 'Invalid header.')
            return self.form_invalid(form)

        # e.g. [['Book title', '540', 'YES', 'description'],]
        mcsv = BookCsv.for_read(table=table)
        if mcsv.is_valid():
            # create book model.
            mcsv.bulk_create()
            return redirect(self.get_success_url)
        # handling errors in templates
        ...
```

## model-csv is a Class-Based Csv Manager.
Column class can define header, an order of columns and a type of value.
And You can define, validate and fix values in ModelCsv methods.
Therefore, you don't have to write spaghetti code in django view anymore.

##  spaghetti code example.
```python

class BookDownloadCsv(generic.View):
    def post(self, **kwargs):
        header = [
            'Book Title',
            'Now On sale',
            'R18',
            'Price',
            'Publish Date',
            'Memo',
            'Description',
            'Csv Output Date',
            # Suppose columns over 20 ...
        ]
        body = [header]
        today = timezone.now().date()
        for book in Book.objects.all():
            title = book.title.replace(N_WORD, '*', len(N_WORD))
            if book.is_restricted_under_18:
                title = '[R18]' + title

            price = f'${book.price or 0}'
            if book.published_at:
                published_at = book.published_at.strftime('%Y-%m-%d')
            else:
                published_at = 'TBD'
                title = '(TBD)' + title  # values are sometimes fixed at several points.
                price += ' (Unsettled)'

            # Suppose each values need to be fixed depends on other values ...
            body += [
                title,
                'YES' if book.is_on_sale else 'NO',
                'YES' if book.is_restricted_under_18 else 'NO',
                price,
                published_at,
                '',
                '',
                today,
                # You have to check if order of values is valid ...
            ]
        return # create csv file and return response.
```

Cool Solution with model-csv

```python
from model_csv import ModelCsv, columns

class BookCsv(ModelCsv):
    # MethodColumn is a column which returns a result of `column_*` method.
    title = columns.MethodColumn(header='Book Title')

    # AttributeColumn is a column which returns an attribute of model.
    is_on_sale = columns.AttributeColumn(header='Now On Sale', to=bool)
    is_restricted_under_18 = columns.AttributeColumn(header='R18', to=bool)
    price = columns.MethodColumn(header='Price')
    published_at = columns.AttributeColumn(header='Publish Date')

    # StaticColumn is a column which always returns `static_value.`
    memo = columns.StaticColumn(header='Memo', static_value='')
    description = columns.StaticColumn(header='Description')
    csv_output_date = columns.StaticColumn(header='Csv Output Date')
    # even if columns over 20, it's not so difficult to manage order.

    class Meta:
        model = Book
        read_mode = False  # if False, then this class cannot call Csv.for_read()
        # if auto_assign is true, column indexed are automatically assigned.
        # Assigned indexes are equal to the declaration order of columns.
        auto_assign = True

    def column_title(self, instance: Book, **kwargs) -> str:
        # Special methods with `column_` prefix are called when write down to a csv file.
        title = instance.title.replace(N_WORD, '*' * len(N_WORD))
        if instance.is_restricted_under_18:
            title = '[R18] ' + title
        if instance.is_restricted_under_18:
            title = '(TBD)' + title
        return title

    def column_published_at(self, instance: Book, **kwargs) -> Union[datetime, stre]:
        pbl_at = instance.published_at
        return pbl_at.strftime('%Y-%m-%d') if pbl_at else 'TBD'

    def column_price(self, instance: Book, **kwargs) -> str:
        price = f'${instance.price or 0}'
        if not instance.published_at:
             price += ' (Unsettled)'

        return price


class BookDownloadView(generic.View):
    # you can keep your view simpler.
    def post(self, **kwargs):
        # call as write mode.
        mcsv = BookCsv.for_write(instances=Book.objects.all())
        # you can insert values after construct ModelCsv.
        mcsv.set_static_column('csv_output_date', timezone.now().date())
        # define filename and file type. Other choices are TsvWriter, ExcelWriter ...
        writer = CsvWriter(file_name='book.csv')
        return mcsv.get_response(writer=writer)
```

## Columns

There are 3 types of columns and 1 decorator.

All these columns work in a same way when read values from a csv file.

(just return a value from row by using an index)

They work differently when write down values to a csv file.

### AttributeColumn
Write down an attribute value
```python
class BookCsv(DjangoCsv):
    title = columns.AttributeColumn(header='Book Title', index=0)

    class Meta:
        model = Book
```
Then, `AttributeColumn` returns `book.title` when create csv file.

If set `attr_name` then `AttributeColumn` changes an attribute it refers to.
```python
class BookCsv(ModelCsv):
    title = columns.AttributeColumn(header='Book Title', index=0)
    official_title = columns.AttributeColumn(
        header='Official Title', attr_name='title', index=1)  # same as title

    class Meta:
        model = Book
```

### MethodColumn
Write down a value which is returned by a method
```python
class BookCsv(ModelCsv):
    full_title = columns.MethodColumn(header='Title', index=0)

    class Meta:
        model = Book

    def column_full_title(self, instance: Book, **kwargs) -> str:
        return f'{instance.title} {instance.subtitle}'

    def field_title(self, value: dict, **kwargs) -> str:
        return value["full_title"].split()[0]

    def field_subtitle(self, value: dict, **kwargs) -> str:
        return value["full_title"].split()[1]
```

If MethodColumn then ModelCsv search methods named `column_<name>` and write down the return values.

### @as_column
@as_column decorator returns a column which works like MethodColumn.
```python
class BookCsv(ModelCsv):

    @columns.as_column(header='Title', index=0)
    def full_title(self, instance: Book, **kwargs) -> str:
        return f'{instance.title} {instance.subtitle}'
```

### StaticColumn
StaticColumn simply returns a static value.
```python
class BookCsv(ModelCsv):
    check = columns.StaticColumn(header='Check Box')  # always returns ''
    title = columns.AttributeColumn(header='Title')
    ...
    phone_number = columns.StaticColumn(header='Phone Number',
                                        static_value='+81 080-0000-0000')
    class Meta:
        model = Book
        auto_assign = True
```

## Meta class of ModelCsv

```python
class BookCsv(ModelCsv):
    class Meta:
        model = Book  # django model
        # automatically assign indexes to columns
        auto_assign = True

        # prohibit to use as read or write mode.
        read_mode = False  # raise Error if .for_read is called
        write_mode = True

        # Convert datetime to str or str to datetime.
        # Column(to=datetime) and auto_convert=True
        datetime_format = '%Y-%m-%d %H:%M:%S'
        date_format = '%Y-%m-%d'
        tzinfo = timezone.utc

        # convert bool to str.
        # Column(to=bool) and auto_convert=True
        show_true = 'yes'
        show_false = 'no'

        # understand values as bool if the value in list.
        # Column(to=bool) and auto_convert=True
        as_true = ['yes', 'Yes']
        as_false = ['no', 'No']

        auto_convert = True

        # The value ModelCsv write down if the value is None
        default_if_none = ''

        # if indexes are not sequence, insert blank columns.
        insert_blank_column = True
```
