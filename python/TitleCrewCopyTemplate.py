from CopyTemplate import CopyTemplate

class TitleCrewCopyTemplate(CopyTemplate):

    def __init__(self, filename, table_dict):
        super().__init__(filename, table_dict)

    def ingest_row(self, row):
        if row["directors"] != "\\N":
            for d in row["directors"].split(","):
                self.get_table_ctx("director").write_row((
                    row["tconst"],
                    d
                ))
        if row["writers"] != "\\N":
            for w in row["writers"].split(","):
                self.get_table_ctx("writer").write_row((
                    row["tconst"],
                    w
                ))
