
from CopyTemplate import CopyTemplate

class TitleEpisodeCopyTemplate(CopyTemplate):

    episodeIdCounter = 0

    schema = {
        "titleEpisode": {
            "episodeId": None,
            "tconst": None,
            "parentTconst": None,
            "seasonNumber": None,
            "episodeNumber": None
        }
    }

    def __init__(self, filename):
        super().__init__(filename)

    def ingest_row(self, row):
        TitleEpisodeCopyTemplate.episodeIdCounter += 1
        self.get_table_ctx("titleEpisode").write_row((
            TitleEpisodeCopyTemplate.episodeIdCounter,
            row["tconst"],
            row["parentTconst"],
            row["seasonNumber"],
            row["episodeNumber"]
        ))
