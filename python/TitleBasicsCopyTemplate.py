from CopyTemplate import CopyTemplate

class TitleBasicsCopyTemplate(CopyTemplate):

    titleTypeIdCounter = 0
    genreIdCounter = 0
    titleType_id_dict = {}
    genre_id_dict = {}

    def __init__(self, filename, table_dict):
        super().__init__(filename, table_dict)

    @classmethod
    def insert_new_titleType(cls, t):
        TitleBasicsCopyTemplate.titleTypeIdCounter += 1
        TitleBasicsCopyTemplate.titleType_id_dict[t] = TitleBasicsCopyTemplate.titleTypeIdCounter

    @classmethod
    def insert_new_genre(cls, g):
        TitleBasicsCopyTemplate.genreIdCounter += 1
        TitleBasicsCopyTemplate.genre_id_dict[g] = TitleBasicsCopyTemplate.genreIdCounter

    def ingest_row(self, row):
        if row["titleType"] != "\\N":
            ttype = row["titleType"]
            if ttype not in TitleBasicsCopyTemplate.titleType_id_dict:
                TitleBasicsCopyTemplate.insert_new_titleType(ttype)
                self.get_table_ctx("titleType").write_row((
                    TitleBasicsCopyTemplate.titleTypeIdCounter,
                    ttype
                ))
        self.get_table_ctx("titleBasics").write_row((
            row["tconst"],
            row["primaryTitle"],
            row["originalTitle"],
            row["isAdult"],
            self.dateify(row["startYear"]),
            self.dateify(row["endYear"]),
            row["runtimeMinutes"],
            self.titleType_id_dict.get(row["titleType"], "\\N")
        ))
        if row["genres"] != "\\N":
            for g in row["genres"].split(","):
                if g not in TitleBasicsCopyTemplate.genre_id_dict:
                    TitleBasicsCopyTemplate.insert_new_genre(g)
                    self.get_table_ctx("genre").write_row((
                        TitleBasicsCopyTemplate.genreIdCounter,
                        g
                    ))
                self.get_table_ctx("titleBasics_genre").write_row((
                    row["tconst"],
                    TitleBasicsCopyTemplate.genre_id_dict[g]
                ))
