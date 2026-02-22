from CopyTemplate import CopyTemplate

class TitleRatingsCopyTemplate(CopyTemplate):

    schema = {
        "ratings": {
            "tconst": None,
            "averageRating": None,
            "numVotes": None
        }
    }

    def __init__(self, filename):
        super().__init__(filename)

    def ingest_row(self, row):
        self.get_table_ctx("ratings").write_row((
            row["tconst"],
            self.pg_null(row["averageRating"]),
            self.pg_null(row["numVotes"])
        ))
