#!/usr/bin/env python3
# Data configuration file

import os
import gzip
import psycopg

BLOCK_SIZE = 4 * 1024 * 1024 * 1024

conn_string = "postgresql://postgres:postgres@postgres:5432/imdb"

copy_dicts = [ 
    {"filename": "title.basics.tsv.gz",
    "table_name": "title_basics_orig",
    "copy_string": """COPY title_basics_orig (
        tconst,
        titleType,
        primaryTitle,
        originalTitle,
        isAdult,
        startYear,
        endYear,
        runtimeMinutes,
        titleTypeId) FROM STDIN"""},
    {"filename": "name.basics.tsv.gz",
    "table_name": "name_basics_orig",
    "copy_string": """COPY name_basics_orig (
        nconst,
        primaryName,
        birthYear,
        deathYear,
        primaryProfession,
        knownForTitles) FROM STDIN"""},
    {"filename": "title.akas.tsv.gz",
    "table_name": "title_akas_orig",
    "copy_string": """COPY title_akas_orig (
        titleId,
        ordering,
        title,
        region,
        language,
        types,
        attributes,
        isOriginalTitle) FROM STDIN"""},
    {"filename": "title.crew.tsv.gz",
    "table_name": "title_crew_orig",
    "copy_string": """COPY title_crew_orig (
        tconst,
        directors,
        writers) FROM STDIN"""},
    {"filename": "title.episode.tsv.gz",
    "table_name": "title_episode_orig",
    "copy_string": """COPY title_episode_orig (
        tconst,
        parentTconst,
        seasonNumber,
        episodeNumber) FROM STDIN"""},
    {"filename": "title.principals.tsv.gz",
    "table_name": "title_principals_orig",
    "copy_string": """COPY title_principals_orig (
        tconst,
        ordering,
        nconst,
        category,
        job,
        characters) FROM STDIN"""},
    {"filename": "title.ratings.tsv.gz",
    "table_name": "title_ratings_orig",
    "copy_string": """COPY title_ratings_orig (
        tconst,
        averageRating,
        numVotes) FROM STDIN"""}
]
    

def handle_files():
    conn = psycopg.connect(conn_string)
    cur = conn.cursor()
    for c in copy_dicts:
        print(f"Processing {c['filename']}", flush=True)
        cur.execute(f"ALTER TABLE {c['table_name']} DISABLE TRIGGER ALL;")
        with gzip.open(os.path.join("/data", c["filename"]), "rt") as d:
            with cur.copy(c["copy_string"]) as cp:
                while data:=d.read(BLOCK_SIZE):
                    cp.write(data)
        cur.execute(f"ALTER TABLE {c['table_name']} ENABLE TRIGGER ALL;")
    conn.commit()
    conn.close()


print("Starting etl process...", flush=True)
handle_files()
print("Etl process completed, exiting.", flush=True)
exit(0)
