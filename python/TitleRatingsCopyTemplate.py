from CopyTemplate import CopyTemplate

class TitleRatingsCopyTemplate(CopyTemplate):

    def __init__(self, filename, table_dict):
        super().__init__(filename, table_dict)

    def ingest_row(self, row):
        self.get_table_ctx("ratings").write_row((
            row["tconst"],
            row["averageRating"],
            row["numVotes"]
        ))
