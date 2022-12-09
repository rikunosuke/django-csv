class ValidationError(Exception):
    """Error while validating data"""

    def __init__(self, *args, label: str | None = None,
                 column_index: int | None = None):
        self.label = label
        self.column_index = column_index
        super().__init__(*args)
