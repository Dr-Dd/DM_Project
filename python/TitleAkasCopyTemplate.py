from CopyTemplate import CopyTemplate

class TitleAkasCopyTemplate(CopyTemplate):

    languageIdCounter = 0
    language_id_dict = {}
    regionIdCounter = 0
    region_id_dict = {}
    akaTypeIdCounter = 0
    akaType_id_dict = {}
    attributeIdCounter = 0
    attribute_id_dict = {}
    titleAkasIdCounter = 0

    schema = {
        "language": {
            "languageId": None,
            "languageName": None
        },
        "region": {
            "regionId": None,
            "regionName": None
        },
        "titleAkas": {
            "akasId": None,
            "titleId": None,
            "ordering": None,
            "title": None,
            "regionId": None,
            "languageId": None,
            "isOriginalTitle": None
        },
        "akaType": {
            "akaTypeId": None,
            "akaTypeName": None
        },
        "akaType_titleAkas": {
            "akaTypeId": None,
            "titleId": None
        },
        "attribute": {
            "attributeId": None,
            "attributeText": None
        },
        "attribute_titleAkas": {
            "attributeId": None,
            "akasId": None
        }
    }

    def __init__(self, filename):
        super().__init__(filename)

    @classmethod
    def insert_new_language(cls, lang):
        TitleAkasCopyTemplate.languageIdCounter += 1
        TitleAkasCopyTemplate.language_id_dict[lang] = TitleAkasCopyTemplate.languageIdCounter

    @classmethod
    def insert_new_region(cls, reg):
        TitleAkasCopyTemplate.regionIdCounter += 1
        TitleAkasCopyTemplate.region_id_dict[reg] = TitleAkasCopyTemplate.regionIdCounter

    @classmethod
    def insert_new_akaType(cls, t):
        TitleAkasCopyTemplate.akaTypeIdCounter += 1
        TitleAkasCopyTemplate.akaType_id_dict[t] = TitleAkasCopyTemplate.akaTypeIdCounter

    @classmethod
    def insert_new_attribute(cls, a):
        TitleAkasCopyTemplate.attributeIdCounter += 1
        TitleAkasCopyTemplate.attribute_id_dict[a] = TitleAkasCopyTemplate.attributeIdCounter

    def ingest_row(self, row):
        TitleAkasCopyTemplate.titleAkasIdCounter += 1
        if row["language"] != "\\N":
            lang = row["language"]
            if lang not in TitleAkasCopyTemplate.language_id_dict:
                self.insert_new_language(lang)
                self.get_table_ctx("language").write_row((
                    TitleAkasCopyTemplate.language_id_dict[lang],
                    lang
                ))
        if row["region"] != "\\N":
            reg = row["region"]
            if reg not in TitleAkasCopyTemplate.region_id_dict:
                self.insert_new_region(reg)
                self.get_table_ctx("region").write_row((
                    TitleAkasCopyTemplate.region_id_dict[reg],
                    reg
                ))
        if row["types"] != "\\N":
            for t in type_split(row["types"]):
                if t not in TitleAkasCopyTemplate.akaType_id_dict:
                    self.insert_new_akaType(t)
                    self.get_table_ctx("akaType").write_row((
                        TitleAkasCopyTemplate.akaType_id_dict[t],
                        t
                    ))
                self.get_table_ctx("akaType_titleAkas").write_row((
                    TitleAkasCopyTemplate.akaType_id_dict[t],
                    TitleAkasCopyTemplate.titleAkasIdCounter
                ))
        self.get_table_ctx("titleAkas").write_row((
            TitleAkasCopyTemplate.titleAkasIdCounter,
            row["titleId"],
            row["ordering"],
            row["title"],
            self.region_id_dict.get(row["region"], "\\N"),
            self.language_id_dict.get(row["language"], "\\N"),
            row["isOriginalTitle"]
        ))
        if row["attributes"] != "\\N":
            for a in attribute_split(row["attributes"]):
                if a not in TitleAkasCopyTemplate.attribute_id_dict:
                    self.insert_new_attribute(a)
                    self.get_table_ctx("attribute").write_row((
                        TitleAkasCopyTemplate.attribute_id_dict[a],
                        a
                    ))
                self.get_table_ctx("attribute_titleAkas").write_row((
                    TitleAkasCopyTemplate.attribute_id_dict[a],
                    TitleAkasCopyTemplate.titleAkasIdCounter
                ))
