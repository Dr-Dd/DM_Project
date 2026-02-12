#!/usr/bin/env python3
# Data configuration file
import time
import os
import gzip
import psycopg
import csv
import re

data_files = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

known_types = {
    "alternative",
    "dvd",
    "festival",
    "imdbDisplay",
    "original",
    "tv",
    "video",
    "working"
}

known_attributes = {
    "16mm release title",
    "16mm rental title",
    "3-D version",
    "3-D video title",
    "8mm release title",
    "added framing sequences and narration in Yiddish",
    "alternative spelling",
    "alternative transliteration",
    "American Mutoscope & Biograph catalog title",
    "anthology series",
    "approximation of original mirrored title",
    "armed forces circuit title",
    "attributes",
    "Bable dialect title",
    "Berlin film festival title",
    "Bilbao festival title",
    "bootleg title",
    "bowdlerized title",
    "cable TV title",
    "Cannes festival title",
    "censored version",
    "closing credits title",
    "complete title",
    "copyright title",
    "correct transliteration",
    "cut version",
    "daytime version title",
    "director's cut",
    "dubbed version",
    "DVD box title",
    "DVD menu title",
    "eighteenth season title",
    "eighth season title",
    "eleventh season title",
    "English translation of working title",
    "english transliteration",
    "expansion title",
    "fake working title",
    "fifteenth season title",
    "fifth part title",
    "fifth season title",
    "first episodes title",
    "first episode title",
    "first part title",
    "first season title",
    "first segment title",
    "fortieth season title",
    "fourteenth season title",
    "fourth part title",
    "fourth season title",
    "game box title",
    "GameCube version",
    "Hakka dialect title",
    "IMAX version",
    "incorrect title",
    "informal alternative title",
    "informal English title",
    "informal literal English title",
    "informal literal title",
    "informal short title",
    "informal title",
    "last season title",
    "late Sunday edition",
    "LD title",
    "literal English title",
    "literal French title",
    "literal title",
    "literal translation of working title",
    "Locarno film festival title",
    "longer version",
    "long new title",
    "long title",
    "Los Angeles premiere title",
    "Los Angeles première title",
    "MIFED title",
    "modern translation",
    "new syndication title",
    "new title",
    "nineteenth season title",
    "ninth season title",
    "non-modified Hepburn romanization",
    "original pilot title",
    "original script title",
    "original subtitled version",
    "orthographically correct title",
    "Pay-TV title",
    "PC version",
    "POLart",
    "poster title",
    "premiere title",
    "première title",
    "pre-release title",
    "promotional abbreviation",
    "promotional title",
    "racier version",
    "recut version",
    "redubbed comic version",
    "reissue title",
    "rerun title",
    "restored version",
    "review title",
    "R-rated version",
    "rumored",
    "second copyright title",
    "second part title",
    "second season title",
    "second segment title",
    "segment title",
    "series title",
    "seventeenth season title",
    "seventh season title",
    "short title",
    "short version",
    "silent version",
    "sixteenth season title",
    "sixth season title",
    "soft porn version",
    "subtitle",
    "summer title",
    "syndication title",
    "teaser title",
    "tenth season title",
    "theatrical title",
    "third and fourth season title",
    "third part title",
    "third season title",
    "third segment title",
    "thirteenth season title",
    "thirtieth season title",
    "thirtyeighth season title",
    "thirtyfifth season title",
    "thirtyfirst season title",
    "thirtyfourth season title",
    "thirtyninth season title",
    "thirtysecond season title",
    "thirtyseventh season title",
    "thirtysixth season title",
    "thirtythird season title",
    "title for episodes with guest hosts",
    "trailer title",
    "transliterated title",
    "TV listings title",
    "twelfth season title",
    "twentieth season title",
    "twentyeighth season title",
    "twentyfifth season title",
    "twentyfirst season title",
    "twentyfourth season title",
    "twentyninth season title",
    "twentysecond season title",
    "twentyseventh season title",
    "twentysixth season title",
    "twentythird season title",
    "unauthorized video title",
    "uncensored intended title",
    "Venice film festival title",
    "video box title",
    "video box title ",
    "video catalogue title",
    "video CD title",
    "videogame episode",
    "weekend title",
    "X-rated version",
    "Yiddish dubbed",
    "YIVO translation"
}

type_pattern = re.compile("|".join(sorted(known_types, key=len, reverse=True)))
attribute_pattern = re.compile("|".join(sorted(known_attributes, key=len, reverse=True)))

# Usage
conn_string = "postgresql://postgres:postgres@postgres:5432/imdb"

def pg_null(v):
    if v is None or v == r"\N":
        return None
    return v

def dateify(y):
    if y == "\\N":
        return None
    return f"{int(y):04d}-01-01"

def load_data():
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            handle_files(cur)
        conn.commit()

def get_id_by_value(data, target_v):
    for id_, v in data:
        if v == target_v:
            return id_
    return None

def type_split(word):
    return type_pattern.findall(word)

def attribute_split(word):
    return attribute_pattern.findall(word)

def handle_files(cur):
    for f in data_files:
        with gzip.open(os.path.join("/data", f), "rt") as d:
            r = csv.DictReader(d, delimiter="\t", quoting=csv.QUOTE_NONE)
            match f:
                case "title.basics.tsv.gz":
                    with (cur.copy("COPY titleType (titleTypeId, titleTypeName) FROM STDIN") as cp_titleType,
                          cur.copy("""COPY titleBasics (
                          tconst,
                          primaryTitle,
                          originalTitle,
                          isAdult,
                          startYear,
                          endYear,
                          runtimeMinutes,
                          titleTypeId) FROM STDIN
                          """) as cp_titleBasics,
                          cur.copy("COPY genre (genreId, genreName) FROM STDIN") as cp_genre,
                          cur.copy("COPY titleBasics_genre (tconst, genreId) FROM STDIN") as cp_titleBasics_genre
                          ):
                        titleType_rows = {}
                        titleTypeId = 1
                        genre_rows = {}
                        genreId = 1
                        for l in r:
                            if l["titleType"] != "\\N" and l["titleType"] not in titleType_rows:
                                titleType_rows[l["titleType"]] = titleTypeId
                                titleTypeId += 1
                                cp_titleType.write_row((
                                    titleType_rows[l["titleType"]], l["titleType"]
                                ))
                            if l["genres"] != "\\N":
                                for g in l["genres"].split(","):
                                    if g not in genre_rows:
                                        genre_rows[l["genre"]] = genreId
                                        genreId += 1
                                        cp_genre.write_row((
                                            genre_rows[l["genre"]], l["genre"]
                                        ))
                                        cp_titleBasics_genre.write_row((
                                            l["tconst"], genre_rows[l["genre"]]
                                        ))
                            cp_titleBasics.write_row((
                                l["tconst"],
                                l["primaryTitle"],
                                l["originalTitle"],
                                l["isAdult"],
                                pg_null(dateify(l["startYear"])),
                                pg_null(dateify(l["endYear"])),
                                pg_null(l["runtimeMinutes"]),
                                pg_null(titleType_rows[l["titleType"]])
                            ))
                case "title.akas.tsv.gz":
                    with(cur.copy("COPY language (languageId, languageName) FROM STDIN") as cp_language,
                         cur.copy("COPY region (regionId, regionName)") as cp_region,
                         cur.copy("""
                         COPY titleAkas (
                         akasId,
                         titleId,
                         ordering,
                         title,
                         regionId,
                         languageId,
                         isOriginalTitle
                         )
                         """) as cp_titleAkas,
                         cur.copy("COPY akaType (akaTypeId, akaTypeName)") as cp_akaType,
                         cur.copy("COPY akaType_titleAkas (akaTypeId, titleId)") as cp_akaType_titleAkas,
                         cur.copy("COPY attribute (attributeId, attributeText)") as cp_attribute,
                         cur.copy("COPY attribute_titleAkas (attributeId, akasId)") as cp_attribute_titleAkas
                         ):
                        language_id_map = {}
                        language_id = 1
                        region_id_map = {}
                        region_id = 1
                        akaType_id_map = {}
                        akaType_id = 1
                        attribute_id_map = {}
                        attribute_id = 1
                        titleAkas_id = 1
                        for l in r:
                            if l["language"] != "\\N"and l["language"] not in language_id_map:
                                language_id_map[l["language"]] = language_id
                                language_id += 1
                                cp_language.write_row((
                                    language_id_map[l["language"]], l["language"]
                                ))
                            if l["region"] != "\\N" and l["region"] not in region_id_map:
                                region_id_map[l["region"]] = region_id
                                region_id += 1
                                cp_region.write_row((
                                    region_id_map[l["region"]], l["region"]
                                ))
                            if l["types"] != "\\N":
                                for t in type_split(l["types"]):
                                    if t not in akaType_id_map:
                                        akaType_id_map[t] = akaType_id
                                        akaType_id += 1
                                        cp_akaType.write_row((akaType_id_map[t], t))
                                        cp_akaType_titleAkas.write_row((
                                            akaType_id_map[t], l["titleId"]
                                        ))
                            cp_titleAkas.write_row((
                                titleAkas_id,
                                 l["titleId"],
                                 l["ordering"],
                                 l["title"],
                                 pg_null(region_id_map[l["region"]]),
                                 pg_null(language_id_map[l["language"]]),
                                 l["isOriginalTitle"]
                            ))
                            if l["attributes"] != "\\N":
                                for t in attribute_split(l["attributes"]):
                                    if t not in attribute_id_map:
                                        attribute_id_map[t] = attribute_id
                                        attribute_id += 1
                                        cp_attribute.write_row((attribute_id_map[t], t))
                                        cp_attribute_titleAkas.write_row((
                                            attribute_id_map[t],
                                            titleAkas_id
                                        ))
                            titleAkas_id += 1




print("Starting etl process...", flush=True)
load_data()
print("Etl process completed, exiting.", flush=True)
exit(0)
