class CopyTemplate:

    _instances = {}
    schema = ""

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]

    def __init__(self, filename):
        self.filename = filename
        self.conn_cur_cp_ctx_dict = {}

    @staticmethod
    def generate_copy_statement(table_name: str, columns: list[str]) -> str:
        return f"COPY {table_name} ({', '.join(columns.keys())}) FROM STDIN"

    @staticmethod
    def dateify(y):
        if y != "\\N":
            return f"{int(y):04d}-01-01"
        return y

    @classmethod
    def get_schema(cls):
        return cls.schema

    def ingest_row(self, row):
        raise NotImplementedError

    def get_table_ctx(self, t):
        return self.conn_cur_cp_ctx_dict[t][4]
