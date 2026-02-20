#!/usr/bin/env python3
# Data configuration file

################################################################
################################################################
########## SCRATCH ALL THIS, THE BEST PROCESS IS: ##############
########## csv -> pg_bulkload -> temp_table -> pg processing -> destroy table ###
########## GET USED TO IT U PU$$Y ##############################
########## PEACE ###############################################
import io
import os
import gzip
import psycopg
import csv
import re

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

def write_buffer_row(buf, *args):
    size = 0
    for a in args:
        size += len(a)
    buf.write("\t".join(map(str, args)) + "\n")
    return size

def pg_null(v):
    if v is None or v == "\\N":
        return None
    return v

def dateify(y):
    if y != "\\N":
        return f"{int(y):04d}-01-01"
    return y

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
                    conn = psycopg.connect(conn_string)
                    cur_titleType = conn.cursor()
                    cur_titleBasics = conn.cursor()
                    cur_genre = conn.cursor()
                    cur_titleBasics_genre = conn.cursor()
                    cur_titleBasics.execute("ALTER TABLE titleBasics DISABLE TRIGGER ALL;")
                    cur_titleBasics_genre.execute("ALTER TABLE titleBasics_genre DISABLE TRIGGER ALL;")
                    with (cur_titleType.copy("COPY titleType (titleTypeId, titleTypeName) FROM STDIN") as cp_titleType,
                          cur_titleBasics.copy("""COPY titleBasics (
                          tconst,
                          primaryTitle,
                          originalTitle,
                          isAdult,
                          startYear,
                          endYear,
                          runtimeMinutes,
                          titleTypeId) FROM STDIN
                          """) as cp_titleBasics,
                          cur_genre.copy("COPY genre (genreId, genreName) FROM STDIN") as cp_genre,
                          cur_titleBasics_genre.copy("COPY titleBasics_genre (tconst, genreId) FROM STDIN") as cp_titleBasics_genre
                          ):
                        print("Connected for titles.", flush=True)
                        titleType_rows = {}
                        titleTypeId = 1
                        genre_rows = {}
                        genreId = 1
                        buf_titleType = io.StringIO()
                        buf_titleBasics = io.StringIO()
                        buf_genre = io.StringIO()
                        buf_titleBasics_genre = io.StringIO()
                        tot_size = 0
                        for l in r:
                            if l["titleType"] != "\\N":
                                if l["titleType"] not in titleType_rows:
                                    titleType_rows[l["titleType"]] = titleTypeId
                                    tot_size += write_buffer_row(
                                        buf_titleType,
                                        titleTypeId,
                                        l["titleType"]
                                    )
                                    titleTypeId += 1
                            tot_size += write_buffer_row(
                                buf_titleBasics,
                                l["tconst"],
                                l["primaryTitle"],
                                l["originalTitle"],
                                l["isAdult"],
                                dateify(l["startYear"]),
                                dateify(l["endYear"]),
                                l["runtimeMinutes"],
                                titleType_rows.get(l["titleType"], "\\N")
                            )
                            if l["genres"] != "\\N":
                                for g in l["genres"].split(","):
                                    if g not in genre_rows:
                                        genre_rows[g] = genreId
                                        tot_size += write_buffer_row(
                                            buf_genre,
                                            genreId,
                                            g
                                        )
                                        genreId += 1
                                    tot_size += write_buffer_row(
                                        buf_titleBasics_genre,
                                        l["tconst"],
                                        genre_rows[g]
                                    )
                            if tot_size >= BLOCK_SIZE:
                                cp_titleType.write(buf_titleType.getvalue())
                                cp_titleBasics.write(buf_titleBasics.getvalue())
                                cp_genre.write(buf_genre.getvalue())
                                cp_titleBasics_genre.write(buf_titleBasics_genre.getvalue())
                                tot_size = 0
                                for b in (buf_titleType, buf_titleBasics, buf_genre, buf_titleBasics_genre):
                                    b.seek(0)
                                    b.truncate(0)
                        cp_titleType.write(buf_titleType.getvalue())
                        cp_titleBasics.write(buf_titleBasics.getvalue())
                        cp_genre.write(buf_genre.getvalue())
                        cp_titleBasics_genre.write(buf_titleBasics_genre.getvalue())
                    conn.commit()
                    cur_titleBasics.execute("ALTER TABLE titleBasics ENABLE TRIGGER ALL;")
                    cur_titleBasics_genre.execute("ALTER TABLE titleBasics_genre ENABLE TRIGGER ALL;")
                    conn.close()

                case "title.akas.tsv.gz":
                    r = csv.DictReader(d, delimiter="\t", quoting=csv.QUOTE_NONE)
                    conn = psycopg.connect(conn_string)
                    cur_language = conn.cursor()
                    cur_region = conn.cursor()
                    cur_titleAkas = conn.cursor()
                    cur_akaType = conn.cursor()
                    cur_akaType_titleAkas = conn.cursor()
                    cur_attribute = conn.cursor()
                    cur_attribute_titleAkas = conn.cursor()
                    cur_titleAkas.execute("ALTER TABLE titleAkas DISABLE TRIGGER ALL;")
                    cur_akaType_titleAkas.execute("ALTER TABLE akaType_titleAkas DISABLE TRIGGER ALL;")
                    cur_attribute_titleAkas.execute("ALTER TABLE attribute_titleAkas DISABLE TRIGGER ALL;")
                    with(cur_language.copy("COPY language (languageId, languageName) FROM STDIN") as cp_language,
                         cur_region.copy("COPY region (regionId, regionName) FROM STDIN") as cp_region,
                         cur_titleAkas.copy("""
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
                        cur_akaType.copy("COPY akaType (akaTypeId, akaTypeName) FROM STDIN") as cp_akaType,
                        cur_akaType_titleAkas.copy("COPY akaType_titleAkas (akaTypeId, titleId) FROM STDIN") as cp_akaType_titleAkas,
                        cur_attribute.copy("COPY attribute (attributeId, attributeText) FROM STDIN") as cp_attribute,
                        cur_attribute_titleAkas.copy("COPY attribute_titleAkas (attributeId, akasId) FROM STDIN") as cp_attribute_titleAkas
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
                        buf_language = io.StringIO()
                        buf_region = io.StringIO()
                        buf_titleAkas = io.StringIO()
                        buf_akaType = io.StringIO()
                        buf_akaType_titleAkas = io.StringIO()
                        buf_attribute = io.StringIO()
                        buf_attribute_titleAkas = io.StringIO()
                        tot_size = 0
                        for l in r:
                            if l["language"] != "\\N":
                                if l["language"] not in language_id_map:
                                    language_id_map[l["language"]] = language_id
                                    tot_size += write_buffer_row(
                                        buf_language,
                                        language_id_map[l["language"]],
                                        l["language"]
                                    )
                                    language_id += 1
                            if l["region"] != "\\N":
                                if l["region"] not in region_id_map:
                                    region_id_map[l["region"]] = region_id
                                    tot_size += write_buffer_row(
                                        buf_region,
                                        region_id_map[l["region"]],
                                        l["region"]
                                    )
                                    region_id += 1
                            if l["types"] != "\\N":
                                for t in type_split(l["types"]):
                                    if t not in akaType_id_map:
                                        akaType_id_map[t] = akaType_id
                                        tot_size += write_buffer_row(
                                            buf_akaType,
                                            akaType_id_map[t],
                                            t
                                        )
                                        akaType_id += 1
                                    tot_size += write_buffer_row(
                                        buf_akaType_titleAkas,
                                        akaType_id_map[t],
                                        titleAkas_id
                                    )
                            tot_size += write_buffer_row(
                                buf_titleAkas,
                                titleAkas_id,
                                 l["titleId"],
                                 l["ordering"],
                                 l["title"],
                                 region_id_map.get(l["region"], "\\N"),
                                 language_id_map.get(l["language"], "\\N"),
                                 l["isOriginalTitle"]
                            )
                            if l["attributes"] != "\\N":
                                for t in attribute_split(l["attributes"]):
                                    if t not in attribute_id_map:
                                        attribute_id_map[t] = attribute_id
                                        tot_size += write_buffer_row(
                                            buf_attribute,
                                            attribute_id_map[t],
                                            t
                                        )
                                        attribute_id += 1
                                    tot_size += write_buffer_row(
                                        buf_attribute_titleAkas,
                                        attribute_id_map[t],
                                        titleAkas_id
                                    )
                            titleAkas_id += 1
                            if tot_size >= BLOCK_SIZE:
                                cp_language.write(buf_language.getvalue())
                                cp_region.write(buf_region.getvalue())
                                cp_titleAkas.write(buf_titleAkas.getvalue())
                                cp_akaType.write(buf_akaType.getvalue())
                                cp_akaType_titleAkas.write(buf_akaType_titleAkas.getvalue())
                                cp_attribute.write(buf_attribute.getvalue())
                                cp_attribute_titleAkas.write(buf_attribute_titleAkas.getvalue())
                                tot_size = 0
                                for b in (buf_titleType,
                                          buf_titleBasics,
                                          buf_genre,
                                          buf_titleBasics_genre):
                                    b.seek(0)
                                    b.truncate(0)
                    conn.commit()
                    cur_titleAkas.execute("ALTER TABLE titleAkas ENABLE TRIGGER ALL;")
                    cur_akaType_titleAkas.execute("ALTER TABLE akaType_titleAkas ENABLE TRIGGER ALL;")
                    cur_attribute_titleAkas.execute("ALTER TABLE attribute_titleAkas ENABLE TRIGGER ALL;")
                    conn.close()




print("Starting etl process...", flush=True)
handle_files()
print("Etl process completed, exiting.", flush=True)
exit(0)
