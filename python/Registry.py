from TitleBasicsCopyTemplate import TitleBasicsCopyTemplate
from NameBasicsCopyTemplate import NameBasicsCopyTemplate
from TitleAkasCopyTemplate import TitleAkasCopyTemplate
from TitleCrewCopyTemplate import TitleCrewCopyTemplate
from TitleEpisodeCopyTemplate import TitleEpisodeCopyTemplate
from TitlePrincipalsCopyTemplate import TitlePrincipalsCopyTemplate
from TitleRatingsCopyTemplate import TitleRatingsCopyTemplate

copy_templates = [
    TitleBasicsCopyTemplate("title.basics.tsv.gz"),
    NameBasicsCopyTemplate("name.basics.tsv.gz"),
    TitleAkasCopyTemplate("title.akas.tsv.gz"),
    TitleCrewCopyTemplate("title.crew.tsv.gz"),
    TitleEpisodeCopyTemplate("title.episode.tsv.gz"),
    TitlePrincipalsCopyTemplate("title.principals.tsv.gz"),
    TitleRatingsCopyTemplate("title.ratings.tsv.gz")
]
