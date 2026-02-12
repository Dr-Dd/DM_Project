-- title.basics.tsv.gz OK
CREATE UNLOGGED TABLE titleType (
  titleTypeId serial PRIMARY KEY,
  titleTypeName text UNIQUE NOT NULL
);
CREATE UNLOGGED TABLE genre (
  genreId serial PRIMARY KEY,
  genreName text UNIQUE NOT NULL
);
CREATE UNLOGGED TABLE titleBasics (
  tconst text PRIMARY KEY,
  primaryTitle text NOT NULL,
  originalTitle text NOT NULL,
  isAdult boolean NOT NULL,
  startYear date,
  endYear date,
  runtimeMinutes int,
  titleTypeId integer REFERENCES titleType(titleTypeId)
);
CREATE UNLOGGED TABLE titleBasics_genre (
  tconst text REFERENCES titleBasics(tconst),
  genreId integer REFERENCES genre(genreId),
  PRIMARY KEY(tconst, genreId)
);


-- name.basics.tsv.gz
CREATE UNLOGGED TABLE nameBasics (
  nconst text PRIMARY KEY,
  primaryName text NOT NULL,
  birthYear date NOT NULL,
  deathYear date
);
CREATE UNLOGGED TABLE profession (
  professionId serial PRIMARY KEY,
  professionName text UNIQUE NOT NULL
);
CREATE UNLOGGED TABLE profession_nameBasics (
  professionId integer REFERENCES profession(professionId),
  nconst text REFERENCES nameBasics(nconst),
  PRIMARY KEY(professionId, nconst)
);
-- Just a junction table
CREATE UNLOGGED TABLE knownForTitles (
  tconst text REFERENCES titleBasics(tconst),
  nconst text REFERENCES nameBasics(nconst),
  PRIMARY KEY (nconst, tconst)
);


-- title.akas.tsv.gz OK
CREATE UNLOGGED TABLE language (
  languageId serial PRIMARY KEY,
  languageName text UNIQUE NOT NULL
);
CREATE UNLOGGED TABLE region (
  regionId serial PRIMARY KEY,
  regionName text UNIQUE NOT NULL
);
CREATE UNLOGGED TABLE titleAkas (
  akasId serial PRIMARY KEY,
  titleId text REFERENCES titleBasics(tconst) NOT NULL,
  ordering smallint NOT NULL,
  title text NOT NULL,
  regionId integer REFERENCES region(regionId),
  languageId integer REFERENCES language(languageId),
  isOriginalTitle boolean NOT NULL
);
CREATE UNLOGGED TABLE akaType (
  akaTypeId serial PRIMARY KEY,
  akaTypeName text UNIQUE NOT NULL
);
CREATE UNLOGGED TABLE akaType_titleAkas (
  akaTypeId integer REFERENCES akaType(akaTypeId),
  titleId integer REFERENCES titleAkas(akasId),
  PRIMARY KEY(akaTypeId, titleId)
);
CREATE UNLOGGED TABLE attribute (
  attributeId serial PRIMARY KEY,
  attributeText text UNIQUE NOT NULL
);
CREATE UNLOGGED TABLE attribute_titleAkas (
  attributeId integer REFERENCES attribute(attributeId),
  akasId integer REFERENCES titleAkas(akasId),
  PRIMARY KEY(attributeId, akasId)
);


-- title.crew.tsv.gz
CREATE UNLOGGED TABLE director (
  tconst text REFERENCES titleBasics(tconst),
  nconst text REFERENCES nameBasics(nconst),
  PRIMARY KEY (tconst, nconst)
);
CREATE UNLOGGED TABLE writer (
  tconst text REFERENCES titleBasics(tconst),
  nconst text REFERENCES nameBasics(nconst),
  PRIMARY KEY (tconst, nconst)
);


-- title.episode.tsv.gz
CREATE UNLOGGED TABLE titleEpisode (
  episodeId serial PRIMARY KEY,
  tconst text REFERENCES titleBasics(tconst),
  parentTconst text REFERENCES titleBasics(tconst),
  seasonNumber int,
  episodeNumber int
);

-- title.principals.tsv.gz
CREATE UNLOGGED TABLE category (
  categoryId serial PRIMARY KEY,
  categoryName text UNIQUE
);
CREATE UNLOGGED TABLE job (
  jobId serial PRIMARY KEY,
  jobName text UNIQUE
);
CREATE UNLOGGED TABLE character (
  characterId serial PRIMARY KEY,
  characterName text UNIQUE
);
CREATE UNLOGGED TABLE titlePrincipals (
  tconst text REFERENCES titleBasics(tconst),
  ordering smallint,
  nconst text REFERENCES nameBasics(nconst),
  categoryId integer REFERENCES category(categoryId),
  jobId integer REFERENCES job(jobId),
  characterId integer REFERENCES character(characterId)
);


-- title.ratings.tsv.gz
CREATE UNLOGGED TABLE ratings (
  tconst text REFERENCES titleBasics(tconst),
  averageRating decimal(3,1),
  check (averageRating >= 0 AND averageRating <= 10),
  numVotes int
);
