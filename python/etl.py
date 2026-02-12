#!/usr/bin/env python3
# Data configuration file
import os
import gzip
import psycopg
import csv
import re
import threading

BLOCK_SIZE = 4 * 1024 * 1024 * 1024

data_files = [
    "name.basics.tsv.gz",
    "title.basics.tsv.gz",
    "title.akas.tsv.gz",
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

def type_split(word):
    return type_pattern.findall(word)

def attribute_split(word):
    return attribute_pattern.findall(word)

def handle_files():
    for f in data_files:
        print(f"Processing {f}", flush=True)
        with gzip.open(os.path.join("/data", f), "rt") as d:
            match f:
                case "title.basics.tsv.gz":
                    r = csv.DictReader(d, delimiter="\t", quoting=csv.QUOTE_NONE)
                    conn_1 = psycopg.connect(conn_string)
                    conn_2 = psycopg.connect(conn_string)
                    conn_3 = psycopg.connect(conn_string)
                    conn_4 = psycopg.connect(conn_string)
                    cur_1 = conn_1.cursor()
                    cur_2 = conn_2.cursor()
                    cur_3 = conn_3.cursor()
                    cur_4 = conn_4.cursor()
                    cur_2.execute("ALTER TABLE titleBasics DISABLE TRIGGER ALL;")
                    cur_4.execute("ALTER TABLE titleBasics_genre DISABLE TRIGGER ALL;")
                    with (cur_1.copy("COPY titleType (titleTypeId, titleTypeName) FROM STDIN") as cp_titleType,
                          cur_2.copy("""COPY titleBasics (
                          tconst,
                          primaryTitle,
                          originalTitle,
                          isAdult,
                          startYear,
                          endYear,
                          runtimeMinutes,
                          titleTypeId) FROM STDIN
                          """) as cp_titleBasics,
                          cur_3.copy("COPY genre (genreId, genreName) FROM STDIN") as cp_genre,
                          cur_4.copy("COPY titleBasics_genre (tconst, genreId) FROM STDIN") as cp_titleBasics_genre
                          ):
                        print("Connected for titles.", flush=True)
                        titleType_rows = {}
                        titleTypeId = 1
                        genre_rows = {}
                        genreId = 1
                        for l in r:
                            if l["titleType"] != "\\N":
                                if l["titleType"] not in titleType_rows:
                                    titleType_rows[l["titleType"]] = titleTypeId
                                    titleTypeId += 1
                                    cp_titleType.write_row((
                                        titleType_rows[l["titleType"]], l["titleType"]
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
                            if l["genres"] != "\\N":
                                for g in l["genres"].split(","):
                                    if g not in genre_rows:
                                        genre_rows[g] = genreId
                                        genreId += 1
                                        cp_genre.write_row((
                                            genre_rows[g], g
                                        ))
                                    cp_titleBasics_genre.write_row((
                                        l["tconst"], genre_rows[g]
                                    ))
                    conn_1.commit()
                    conn_2.commit()
                    conn_3.commit()
                    conn_4.commit()
                    cur_2.execute("ALTER TABLE titleBasics ENABLE TRIGGER ALL;")
                    cur_4.execute("ALTER TABLE titleBasics_genre ENABLE TRIGGER ALL;")
                case "title.akas.tsv.gz":
                    r = csv.DictReader(d, delimiter="\t", quoting=csv.QUOTE_NONE)
                    conn_1 = psycopg.connect(conn_string)
                    conn_2 = psycopg.connect(conn_string)
                    conn_3 = psycopg.connect(conn_string)
                    conn_4 = psycopg.connect(conn_string)
                    conn_5 = psycopg.connect(conn_string)
                    conn_6 = psycopg.connect(conn_string)
                    conn_7 = psycopg.connect(conn_string)
                    cur_1 = conn_1.cursor()
                    cur_2 = conn_2.cursor()
                    cur_3 = conn_3.cursor()
                    cur_4 = conn_4.cursor()
                    cur_5 = conn_5.cursor()
                    cur_6 = conn_6.cursor()
                    cur_7 = conn_7.cursor()
                    cur_3.execute("ALTER TABLE titleAkas DISABLE TRIGGER ALL;")
                    cur_5.execute("ALTER TABLE akaType_titleAkas DISABLE TRIGGER ALL;")
                    cur_7.execute("ALTER TABLE attribute_titleAkas DISABLE TRIGGER ALL;")
                    with(cur_1.copy("COPY language (languageId, languageName) FROM STDIN") as cp_language,
                         cur_2.copy("COPY region (regionId, regionName) FROM STDIN") as cp_region,
                         cur_3.copy("""
                         COPY titleAkas (
                         akasId,
                         titleId,
                         ordering,
                         title,
                         regionId,
                         languageId,
                         isOriginalTitle
                         ) FROM STDIN
                         """) as cp_titleAkas,
                        cur_4.copy("COPY akaType (akaTypeId, akaTypeName) FROM STDIN") as cp_akaType,
                        cur_5.copy("COPY akaType_titleAkas (akaTypeId, titleId) FROM STDIN") as cp_akaType_titleAkas,
                        cur_6.copy("COPY attribute (attributeId, attributeText) FROM STDIN") as cp_attribute,
                        cur_7.copy("COPY attribute_titleAkas (attributeId, akasId) FROM STDIN") as cp_attribute_titleAkas
                         ):
                        print("Connected for akas.")
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
                            if l["language"] != "\\N":
                                if l["language"] not in language_id_map:
                                    language_id_map[l["language"]] = language_id
                                    language_id += 1
                                    cp_language.write_row((
                                        language_id_map[l["language"]], l["language"]
                                    ))
                            if l["region"] != "\\N":
                                if l["region"] not in region_id_map:
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
                                        akaType_id_map[t], titleAkas_id
                                    ))
                            cp_titleAkas.write_row((
                                titleAkas_id,
                                 l["titleId"],
                                 l["ordering"],
                                 l["title"],
                                 pg_null(region_id_map.get(l["region"])),
                                 pg_null(language_id_map.get(l["language"])),
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
                    conn_1.commit()
                    conn_2.commit()
                    conn_3.commit()
                    conn_4.commit()
                    conn_5.commit()
                    conn_6.commit()
                    conn_7.commit()
                    cur_3.execute("ALTER TABLE titleAkas ENABLE TRIGGER ALL;")
                    cur_5.execute("ALTER TABLE akaType_titleAkas ENABLE TRIGGER ALL;")
                    cur_7.execute("ALTER TABLE attribute_titleAkas ENABLE TRIGGER ALL;")




print("Starting etl process...", flush=True)
handle_files()
print("Etl process completed, exiting.", flush=True)
exit(0)
