from CopyTemplate import CopyTemplate

class NameBasicsCopyTemplate(CopyTemplate):

    professionIdCounter = 0
    professionName_id_dict = {}

    def __init__(self, filename, table_dict):
        super().__init__(filename, table_dict)

    @classmethod
    def insert_new_profession(cls, p):
        NameBasicsCopyTemplate.professionIdCounter += 1
        NameBasicsCopyTemplate.professionName_id_dict[p] = NameBasicsCopyTemplate.professionIdCounter

    def ingest_row(self, row):
        self.get_table_ctx("nameBasics").write_row((
            row["nconst"],
            row["primaryName"],
            self.dateify(row["birthYear"]),
            self.dateify(row["deathYear"])
        ))
        if row["primaryProfession"] != "\\N":
            for p in row["primaryProfession"].split(","):
                if p not in NameBasicsCopyTemplate.professionName_id_dict:
                    self.insert_new_profession(p)
                    self.get_table_ctx("profession").write_row((
                        NameBasicsCopyTemplate.professionIdCounter,
                        p
                    ))
                self.get_table_ctx("profession_nameBasics").write_row((
                    NameBasicsCopyTemplate.professionName_id_dict[p],
                    row["nconst"]
                ))
        if row["knownForTitles"] != "\\N":
            for tit in row["knownForTitles"].split(","):
                self.get_table_ctx("knownForTitles").write_row((
                    tit,
                    row["nconst"]
                ))
