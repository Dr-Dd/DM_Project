
from CopyTemplate import CopyTemplate

class TitleEpisodeCopyTemplate(CopyTemplate):

    episodeIdCounter = 0

    def __init__(self, filename, table_dict):
        super().__init__(filename, table_dict)

    def ingest_row(self, row):
        TitleEpisodeCopyTemplate.episodeIdCounter += 1
        self.get_table_ctx("titleEpisode").write_row((
            TitleEpisodeCopyTemplate.episodeIdCounter,
            row["tconst"],
            row["parentTconst"],
            row["seasonNumber"],
            row["episodeNumber"]
        ))
