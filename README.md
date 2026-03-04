# IMDb Dataset Comparison: RDBMS vs. Triplestore

This project provides a comparative environment for representing and querying IMDb’s non-commercial datasets using two distinct database paradigms: **Relational (PostgreSQL)** and **Graph/RDF (Apache Jena Fuseki)**.

The entire infrastructure is orchestrated via **Docker Compose**, providing a seamless way to ingest, store, and query the same source data in two different formats.

* **Automated Ingestion:** Converts raw IMDb TSV files into SQL-compatible formats and RDF Triples.
* **Dual Endpoints:**
  * **PostgreSQL:** Accessible via Adminer for SQL queries.
  * **Apache Jena Fuseki:** Provides a SPARQL endpoint for graph traversal.
    
---

## Architecture & Setup

### 1. Relational Database (PostgreSQL)

The PostgreSQL dataset is generated and piped via the **COPY protocol** during the initial conversion at startup.

* **Note:** This step is automatically skipped if the database volume is already populated.
* **Access:** [Adminer](http://localhost:8080)
* **Credentials:** `postgres:postgres`
* **Docker hostname**: `postgres`
* **Database Name:** `imdb`

### 2. Triplestore (Apache Jena Fuseki)

The RDF dataset is built using `tdb2.xloader`.

* **Data Loading:** Currently, data needs to be loaded into the named volume after local ingestion.
* **Customization:** You can modify the `compose.yml` to include your own loading image if you wish to automate the TDB2 generation within the stack.
* **Access:** [YASGUI](http://yasgui.org/short/HC0nn7yI0Q) with custom endpoint `http://localhost:3030/ds`

---

## Quick Start

1. **Clone the repository:**
   
   ```bash
   git clone https://github.com/Dr-Dd/DM_Project.git
   cd DM_Project
   ```

2. **Generate N-Triples**
   
   ```bash
   docker compose up java
   ```

3. **Install Apache Jena Fuseki**

   Install a Jena Fuseki distribution to have access to the tdb2 loaders.

5. **Generate the Jena Fuseki Database**
   
   ```bash
   tdb2.xloader --threads=$(nproc --ignore=1) --loc data/DB2 data/*.nt.gz
   ```
   
7. **Copy the Graph Database to Jena Fuseki**
   
   ```bash
   docker run --rm -v $(pwd)/data:/src -v dm_project_fusekidatabases:/dest alpine cp -av /src/DB2 /dest/
   ```
   
9. **Run the compose**
    
   ```bash
   docker compose up
   ```
   
11. **Explore your data**
    
    Here are some queries to get you started:
    * [YASGUI](http://yasgui.org/short/ISP_oz9k0w)
    ```SPARQL
    # SPARQL
    PREFIX schema: <https://schema.org/>
    
    SELECT ?creativeWork ?director ?directorName ?workName ?date
    WHERE {
      ?director a schema:Person ;
                  schema:name ?directorName .
      ?creativeWork schema:director ?director ;
                    schema:name ?workName ;
                    schema:datePublished ?date .
      FILTER(?directorName = "Nanni Moretti")
    } ORDER BY ?date
    ```
    * [Adminer](http://localhost:8080/?pgsql=postgres&username=postgres&db=imdb&ns=public&sql=SELECT%20%0A%20%20tb.tconst%20AS%20%22creativeWork%22%2C%20%0A%20%20d.nconst%20AS%20%22director%22%2C%0A%20%20nb.primaryName%20AS%20%22directorName%22%2C%20%0A%20%20tb.primaryTitle%20AS%20%22workName%22%2C%20%0A%20%20tb.startYear%20AS%20%22date%22%0AFROM%20nameBasics%20nb%0AJOIN%20director%20d%20ON%20nb.nconst%20%3D%20d.nconst%0AJOIN%20titleBasics%20tb%20ON%20d.tconst%20%3D%20tb.tconst%0AJOIN%20titleAkas%20ta%20ON%20tb.tconst%20%3D%20ta.titleId%0AWHERE%20nb.primaryName%20%3D%20%27Nanni%20Moretti%27%20AND%20ta.isOriginalTitle%20%3D%20True%0AORDER%20BY%20tb.startYear%20ASC)
    ```SQL
    # SQL
    SELECT tb.tconst AS "creativeWork", 
           d.nconst AS "director",
           nb.primaryName AS "directorName", 
           tb.primaryTitle AS "title", 
           tb.startYear AS "date"
    FROM nameBasics nb
    JOIN director d ON nb.nconst = d.nconst
    JOIN titleBasics tb ON d.tconst = tb.tconst
    JOIN titleAkas ta ON tb.tconst = ta.titleId
    WHERE nb.primaryName = 'Nanni Moretti' AND ta.isOriginalTitle = True
    ORDER BY tb.startYear ASC
    ```
---

##  License

This project uses IMDb’s Non-Commercial datasets. Please ensure compliance with [IMDb's Terms of Use](https://www.imdb.com/interfaces/).
