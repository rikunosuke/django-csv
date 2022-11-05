# django-csv
django-csv makes it easy to treat csv, tsv and excel file.

# How to use django-csv.
```python3
class Book(models.Model):
    title = models.CharField(max_length=100)
    price = models.PositiveIntegerField(null=True, blank=True)
    is_on_sale = models.BooleanField(default=True)
    description = models.TextField()

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


# ModelCsv for Book class.
class BookCsv(ModelCsv):
    class Meta:
        model = Book
        # if you defined fields then ModelCsv create columns automatically.
        fields = '__all__'

        
# Create list or dict from queryset
book_csv = BookCsv.for_write(instances=Book.objects.all())
book_csv.get_table(header=True)
>> [['title', 'price', 'is_on_sale', 'description'], ['Book title', '540', 'YES', 'description']]
book_csv.get_as_dict()
>> [{'title': 'Book title', 'price': '540', 'is_on_sale': 'YES', 'description': 'description'}]

# Download csv file in django view.
class BookDownloadView(generic.View):
    def get(self, **kwargs):
        book_csv = BookCsv.for_write(instance=Book.objects.all())
        writer = CsvWriter(file_name='book.csv')
        return book_csv.get_response(writer=writer)

# Upload csv file in django view.
class BookUploadView(generic.View):
    def post(self, **kwargs):
        # table is 2D list.
        # e.g. [['Book title', '540', 'YES', 'description'],]
        table = CsvReader(file=file_object, table_starts_from=1).get_table()
        book_csv = BookCsv.for_read(table=table)
        # create book model.
        book_csv.bulk_create()
        return redirect(self.get_success_url)
```

### django-csv is a Class-Based Csv Manager.
ModelCsv can define header, column order and type of value as Class Attribute.
And You can define read and write method in same class.
Therefore, you don't have to write spaghetti code in django view anymore.

Annoying spaghetti code example.
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
        for book in Book.objects.all():
            title = book.title.replace(N_WORD, '*', len(N_WORD))
            if book.is_restricted_under_18:
                title = '[R18]' + title

            if book.published_at:
                published_at = book.published_at.strftime('%Y-%m-%d')
            else:
                published_at = 'TBD'
                title = '(TBD)' + title  # !! values are sometimes fixed at surprising points.
            today = timezone.now().date()
            # Suppose each column need to fix depends on other values ... 
            body += [
                title,
                'Yes' if book.is_on_sale else '',
                'Yes' if book.is_restricted_under_18 else '',
                book.price,
                published_at,
                '',
                '',
                today,
                # You have to check if order is valid for yourself ... 
            ]
        return # create csv file and return response.
```

Cool Solution with django-csv
```python
class BookCsv(ModelCsv):
    # headers and column order are defined as class variable. 
    title = columns.AttributeColumn(header='Book Title')
    is_on_sale = columns.AttributeColumn(header='Now On Sale', to=bool)
    is_restricted_under_18 = columns.AttributeColumn(header='R18', to=bool)
    price = columns.AttributeColumn(header='Price', to=int)
    published_at = columns.AttributeColumn(header='Publish Date')
    memo = columns.StaticColumn(header='Memo', stativ_value='')
    description = columns.StaticColumn(header='Description', static_value='')
    csv_output_date = columns.StaticColumn(header='Csv Output Date')
    # even if columns over 20, it's not so difficult to manage order.

    class Meta:
        model = Book
        read_mode = False  # if False, then this class cannot call Csv.for_read()
        # if auto_assign is true, column index is automatically assigned.
        # Assigned index is equal to the declaration order of columns.
        auto_assign = True
        show_true = 'Yes'
        show_false = ''  # return blank if to=bool and value is False

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


class BookDownloadView(generic.View):
    # you can keep your view simpler.
    def post(self, **kwargs):
        # call as write mode.
        book_csv = BookCsv.for_write(instances=Book.objects.all())
        # you can insert value after construct ModelCsv.
        book_csv.set_static_column('csv_output_date', timezone.now().date())
        # define filename and file type. Other choices are TsvWriter, ExcelWriter ...
        writer = CsvWriter(file_name='book.csv')
        return book_csv.get_response(writer=writer)
```


## ForeignKey?