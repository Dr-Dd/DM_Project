#!/usr/bin/env python3
# Data configuration file
import time
import os
import gzip
import psycopg
import csv

data_files = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz", 
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

# Usage
conn_string = "postgresql://postgres:postgres@postgres:5432/imdb"

def pg_null(v):
    if v == "\\N":
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

def handle_files(cur):
    for f in data_files:
        with gzip.open(os.path.join("/data", f), "rt") as d:
            r = csv.DictReader(d, delimiter="\t", quoting=csv.QUOTE_NONE)
            match f:
                case "title.basics.tsv.gz":
                    titleType_rows = []
                    titleTypeId = 1
                    titleBasics_rows = []
                    genre_rows = []
                    genreId = 1
                    titleBasics_genre_rows = []
                    for l in r:
                        if not any(s == l["titleType"] for _, s in titleType_rows):
                            titleType_rows.append((titleTypeId, l["titleType"]))
                            titleTypeId += 1
                        titleBasics_rows.append(
                            (l["tconst"],
                             l["primaryTitle"],
                             l["originalTitle"],
                             l["isAdult"],
                             pg_null(dateify(l["startYear"])),
                             pg_null(dateify(l["endYear"])),
                             pg_null(l["runtimeMinutes"]),
                             pg_null(get_id_by_value(titleType_rows, l["titleType"])))
                        )
                        # Genre table
                        if l["genres"] and l["genres"] != "\\N":
                            for g in l["genres"].split(","):
                                if not any(s == g for _, s in genre_rows):
                                    genre_rows.append((genreId, g))
                                    genreId += 1
                            # Junction Table
                            for g in l["genres"].split(","):
                                titleBasics_genre_rows.append((l["tconst"], get_id_by_value(genre_rows, g)))
                    with cur.copy("COPY titleType (titleTypeId, titleTypeName) FROM STDIN") as copy:
                        print("Copying into titleType", flush=True)
                        for row in titleType_rows:
                            copy.write_row(row)
                    with cur.copy(
                        """COPY titleBasics (
                           tconst, 
                           primaryTitle, 
                           originalTitle, 
                           isAdult, 
                           startYear, 
                           endYear, 
                           runtimeMinutes, 
                           titleTypeId
                           ) FROM STDIN""") as copy:
                        print("Copying into titleBasics", flush=True)
                        for row in titleBasics_rows:
                            try:
                                copy.write_row(row)
                            except Exception as e:
                                print(row, flush=True)
                    with cur.copy("COPY genre (genreId, genreName) FROM STDIN") as copy:
                        print("Copying into genre", flush=True)
                        for row in genre_rows:
                            copy.write_row(row)
                    with cur.copy("COPY titleBasics_genre (tconst, genreId) FROM STDIN") as copy:
                        print("Copying into titleBasics_genre", flush=True)
                        for row in titleBasics_genre_rows:
                            copy.write_row(row)


print("Starting etl process...", flush=True)
load_data()
print("Etl process completed, exiting.", flush=True)
exit(0)
