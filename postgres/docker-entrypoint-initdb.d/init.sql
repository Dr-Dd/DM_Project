-- title.basics.tsv.gz OK
CREATE TABLE titleType (
  titleTypeId serial PRIMARY KEY,
  titleTypeName text UNIQUE NOT NULL
);
CREATE TABLE genre (
  genreId serial PRIMARY KEY,
  genreName text UNIQUE NOT NULL
);
CREATE TABLE titleBasics (
  tconst text PRIMARY KEY,
  primaryTitle text NOT NULL,
  originalTitle text NOT NULL,
  isAdult boolean NOT NULL,
  startYear date,
  endYear date,
  runtimeMinutes int,
  titleTypeId serial REFERENCES titleType(titleTypeId)
);
CREATE TABLE titleBasics_genre (
  tconst text REFERENCES titleBasics(tconst),
  genreId serial REFERENCES genre(genreId),
  PRIMARY KEY(tconst, genreId)
);


-- name.basics.tsv.gz
CREATE TABLE nameBasics (
  nconst text PRIMARY KEY,
  primaryName text NOT NULL,
  birthYear date NOT NULL,
  deathYear date
);
CREATE TABLE profession (
  professionId serial PRIMARY KEY,
  professionName text UNIQUE NOT NULL
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


-- title.akas.tsv.gz OK
CREATE TABLE language (
  languageId serial PRIMARY KEY,
  languageName text UNIQUE NOT NULL
);
CREATE TABLE region (
  regionId serial PRIMARY KEY,
  regionName text UNIQUE NOT NULL
);
CREATE TABLE titleAkas (
  akasId serial PRIMARY KEY,
  titleId text REFERENCES titleBasics(tconst) NOT NULL,
  ordering smallint NOT NULL,
  title text NOT NULL,
  regionId serial REFERENCES region(regionId),
  languageId serial REFERENCES language(languageId),
  isOriginalTitle boolean NOT NULL
);
CREATE TABLE akaType (
  akaTypeId serial PRIMARY KEY,
  akaTypeName text UNIQUE NOT NULL
);
CREATE TABLE akaType_titleAkas (
  akaTypeId serial REFERENCES akaType(akaTypeId),
  titleId serial REFERENCES titleAkas(akasId),
  PRIMARY KEY(akaTypeId, titleId)
);
CREATE TABLE attribute (
  attributeId serial PRIMARY KEY,
  attributeText text UNIQUE NOT NULL
);
CREATE TABLE attribute_titleAkas (
  attributeId serial REFERENCES attribute(attributeId),
  akasId serial REFERENCES titleAkas(akasId),
  PRIMARY KEY(attributeId, akasId)
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
);


-- title.episode.tsv.gz
CREATE TABLE titleEpisode (
  episodeId serial PRIMARY KEY,
  tconst text REFERENCES titleBasics(tconst),
  parentTconst text REFERENCES titleBasics(tconst),
  seasonNumber int,
  episodeNumber int
);

-- title.principals.tsv.gz
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
CREATE TABLE titlePrincipals (
  tconst text REFERENCES titleBasics(tconst),
  ordering smallint,
  nconst text REFERENCES nameBasics(nconst),
  categoryId serial REFERENCES category(categoryId),
  jobId serial REFERENCES job(jobId),
  characterId serial REFERENCES character(characterId)
);


-- title.ratings.tsv.gz
CREATE TABLE ratings (
  tconst text REFERENCES titleBasics(tconst),
  averageRating decimal(3,1),
  check (averageRating >= 0 AND averageRating <= 10),
  numVotes int
);
