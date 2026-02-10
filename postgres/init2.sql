-- name.basics.tsv.gz
CREATE TABLE nameBasics (
  nconst text PRIMARY KEY,
  primaryName text IS NOT NULL,
  birthYear date IS NOT NULL,
  deathYear date, 
);
CREATE TABLE profession (
  professionId text PRIMARY KEY,
  professionName text UNIQUE IS NOT NULL
);
CREATE TABLE profession_nameBasics (
  professionId serial REFERENCES profession(professionId),
  nconst text REFERENCES nameBasics(nconst),
  PRIMARY KEY(professionId, nconst)
);
-- Just a junction table
CREATE TABLE knownForTitles (
  tconst text REFERENCES titleBasics(tconst),
  nconst text REFERENCES nameBasics(nconst),
  PRIMARY KEY (nconst, tconst)
);


-- title.basics.tsv.gz OK
CREATE TABLE titleBasics (
  tconst text PRIMARY KEY,
  primaryTitle text IS NOT NULL,
  originalTitle text IS NOT NULL,
  isAdult boolean IS NOT NULL,
  startYear date IS NOT NULL,
  endYear date,
  runtimeMinutes smallint,
  titleType serial REFERENCES titleType(titleTypeId)
);
CREATE TABLE genre (
  genreId smallserial PRIMARY KEY,
  name text UNIQUE IS NOT NULL
);
CREATE TABLE titleBasics_genre (
  tconst text REFERENCES titleBasics(tconst),
  genreId serial REFERENCES genre(genreId),
  PRIMARY KEY(tconst, genreId)
);
CREATE TABLE titleType (
  titleTypeId serial PRIMARY KEY,
  titleTypeName text UNIQUE IS NOT NULL
)


-- title.akas.tsv.gz OK
CREATE TABLE titleAkas (
  akasId serial PRIMARY KEY,
  titleId text REFERENCES titleBasics(tconst),
  ordering smallint IS NOT NULL,
  title text IS NOT NULL,
  regionId serial REFERENCES region(regionId),
  languageId serial REFERENCES language(languageId),
  isOriginalTitle boolean IS NOT NULL
);
CREATE TABLE akaType (
  akaTypeId smallint PRIMARY KEY,
  akaTypeName text UNIQUE IS NOT NULL
);
INSERT INTO akaType VALUES
  (1, 'alternative'),
  (2, 'dvd'),
  (3, 'festival'),
  (4, 'tv'),
  (5, 'video'),
  (6, 'working'),
  (7, 'original'),
  (8, 'imdbDisplay');
CREATE TABLE akaType_titleAkas (
  akaTypeId smallint REFERENCES akaType(akaTypeId),
  titleId serial REFERENCES titleAkas(akasId),
  PRIMARY KEY(akaTypeId, titleId)
);
CREATE TABLE attribute (
  attributeId serial PRIMARY KEY,
  attributeText text UNIQUE IS NOT NULL
);
CREATE TABLE attribute_titleAkas (
  attributeId serial REFERENCES attribute(attributeId),
  akasId serial REFERENCES titleAkas(akasId)
  PRIMARY KEY(attributeId, akasId)
);
CREATE TABLE region (
  regionId serial PRIMARY KEY,
  regionName text UNIQUE IS NOT NULL
);
CREATE TABLE language (
  languageId serial PRIMARY KEY,
  languageName text UNIQUE IS NOT NULL
);

-- title.crew.tsv.gz
CREATE TABLE director (
  tconst text REFERENCES titleBasics(tconst),
  nconst text REFERENCES nameBasics(nconst),
  PRIMARY KEY (tconst, nconst)
);
CREATE TABLE writer (
  tconst text REFERENCES titleBasics(tconst),
  nconst text REFERENCES nameBasics(nconst),
  PRIMARY KEY (tconst, nconst)
)


-- title.episode.tsv.gz
CREATE TABLE titleEpisode (
  episodeId serial PRIMARY KEY,
  tconst text REFERENCES titleBasics(tconst),
  parentTconst text REFERENCES titleBasics(tconst),
  seasonNumber smallint,
  episodeNumber smallint
);

-- title.principals.tsv.gz
CREATE TABLE titlePrincipals (
  tconst text REFERENCES titleBasics(tconst),
  ordering smallint,
  nconst text REFERENCES nameBasics(nconst),
  categoryId serial REFERENCES category(categoryId),
  jobId serial REFERENCES job(jobId),
  characterId serial REFERENCES character(characterId)
);
CREATE TABLE category (
  categoryId serial PRIMARY KEY,
  categoryName text UNIQUE
);
CREATE TABLE job (
  jobId serial PRIMARY KEY,
  jobName text UNIQUE
);
CREATE TABLE character (
  characterId serial PRIMARY KEY,
  characterName text UNIQUE 
);


-- title.ratings.tsv.gz
CREATE TABLE ratings (
  tconst text REFERENCES titleBasics(tconst),
  averageRating decimal(3,1),
  check (averageRating >= 0 AND averageRating <= 10),
  numVotes int
);

