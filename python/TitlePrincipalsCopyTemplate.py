from CopyTemplate import CopyTemplate

class TitlePrincipalsCopyTemplate(CopyTemplate):

    categoryIdCounter = 0
    categoryName_id_dict = {}
    jobIdCounter = 0
    jobName_id_dict = {}
    characterIdCounter = 0
    characterName_id_dict = {}

    schema = {
        "category": {
            "categoryId": None,
            "categoryName": None
        },
        "job": {
            "jobId": None,
            "jobName": None
        },
        "character": {
            "characterId": None,
            "characterName": None
        },
        "titlePrincipals": {
            "tconst": None,
            "ordering": None,
            "nconst": None,
            "categoryId": None,
            "jobId": None,
            "characterId": None
        }
    }

    def __init__(self, filename):
        super().__init__(filename)

    @classmethod
    def insert_new_category(cls, cat):
        TitlePrincipalsCopyTemplate.categoryIdCounter += 1
        TitlePrincipalsCopyTemplate.categoryName_id_dict[cat] = TitlePrincipalsCopyTemplate.categoryIdCounter

    @classmethod
    def insert_new_job(cls, j):
        TitlePrincipalsCopyTemplate.jobIdCounter += 1
        TitlePrincipalsCopyTemplate.jobName_id_dict[j] = TitlePrincipalsCopyTemplate.jobIdCounter

    @classmethod
    def insert_new_character(cls, c):
        TitlePrincipalsCopyTemplate.characterIdCounter += 1
        TitlePrincipalsCopyTemplate.characterName_id_dict[c] = TitlePrincipalsCopyTemplate.characterIdCounter

    def ingest_row(self, row):
        if row["category"] not in TitlePrincipalsCopyTemplate.categoryName_id_dict:
            cat = row["category"]
            TitlePrincipalsCopyTemplate.insert_new_category(cat)
            self.get_table_ctx("category").write_row((
                TitlePrincipalsCopyTemplate.categoryIdCounter,
                cat
            ))
        if row["job"] != "\\N":
            j = row["job"]
            if j not in TitlePrincipalsCopyTemplate.jobName_id_dict:
                TitlePrincipalsCopyTemplate.insert_new_job(j)
                self.get_table_ctx("job").write_row((
                    TitlePrincipalsCopyTemplate.jobIdCounter,
                    j
                ))
        if row["characters"] != "\\N":
            c = row["characters"].strip('[]"')
            if c not in TitlePrincipalsCopyTemplate.characterName_id_dict:
                TitlePrincipalsCopyTemplate.insert_new_character(c)
                self.get_table_ctx("character").write_row((
                    TitlePrincipalsCopyTemplate.characterIdCounter,
                    c
                ))
        self.get_table_ctx("titlePrincipals").write_row((
            row["tconst"],
            row["ordering"],
            row["nconst"],
            TitlePrincipalsCopyTemplate.categoryName_id_dict.get(row["category"], None),
            TitlePrincipalsCopyTemplate.jobName_id_dict.get(row["job"], None),
            TitlePrincipalsCopyTemplate.characterName_id_dict.get(row["characters"].strip('[]"'), None)
        ))
