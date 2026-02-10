create table name (
  nconst bpchar primary key
  primaryName bpchar not null
  birthYear date not null
  deathYear date
)

create table title (
  tconst bpchar primary key
  primaryTitle bpchar
  originalTitle bpchar
  isAdult boolean
  startYear date not null
  endYear date
  runtimeMinutes smallint
  typeId references ttype(typeId)
)

create table ttype (
  ttypeId serial primary key
  ttypeName bpchar
)

create table title_genre (
  tconst bpchar references title(tconst)
  genreId serial references genre(genreId)
)

create table genre (
  genreId serial primary key
  tgenre bpchar
)

create table aka (
  akaId serial primary id
  tconst references title(tconst)
  ordering smallint
  aka bpchar
)

create table region_aka (
  akaId references aka(akaId)
  regionId references region(regionId)
)

create table region (
  regionId serial primary key
  region bpchar
)

create table language_aka (
  akaId references aka(akaId)
  languageId references language(languageId)
)

create table language (
  languageId serial primary key
  language bpchar
)

create table nconst_tconst (
  nconst bpchar references name(nconst)
  tconst bpchar references title(tconst)
)

create table title_rating (
  tconst bpchar references title(nconst)
  averageRating numeric(3,1)
  numvotes integer
)

create table title_principals (
  tconst bpchar references title(tconst)
  nconst bpchar references name(nconst)
  ordering smallint
  categoryId references category(categoryId)
)

create table category (
  categoryId serial primary key
  categoryName bpchar
)

create table job (
  jobId serial primary key
  jobName bpchar
  categoryId references category(categoryId)
)
