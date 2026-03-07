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

2. **Download datasets**

   Go to [IMDb's Non-Commercial Datasets](https://datasets.imdbws.com/) page and download each `*.tsv.gz` file, create a `data` directory in the project root and move the archives to it.

3. **Generate N-Triples**

   ```bash
   docker compose up java
   ```

4. **Install Apache Jena Fuseki**

   Install a Jena Fuseki distribution to have access to the tdb2 loaders.

5. **Generate the Jena Fuseki Database**

   ```bash
   tdb2.xloader --threads=$(nproc --ignore=1) --loc data/DB2 data/*.nt.gz
   ```

6. **Copy the Graph Database to Jena Fuseki**

   ```bash
   docker run --rm -v $(pwd)/data:/src -v dm_project_fusekidatabases:/dest alpine cp -av /src/DB2 /dest/
   ```

7. **Run the compose**

   ```bash
   docker compose up
   ```

8. **Explore your data**

    Here are some queries to get you started:
    * [YASGUI](https://yasgui.org/#query=PREFIX+schema%3A+%3Chttps%3A%2F%2Fschema.org%2F%3E%0A%0ASELECT+%3FcreativeWork+%3FdirectorName+%3FworkName+%3Fdate%0AWHERE+%7B%0A++VALUES+%3FdirectorName+%7B+%22Nanni+Moretti%22+%7D%0A++%3Fdirector+schema%3Aname+%3FdirectorName+.%0A++%3FcreativeWork+schema%3Adirector+%3Fdirector+%3B%0A+++++++++++++++schema%3Aname+%3FworkName+%3B%0A+++++++++++++++schema%3AdatePublished+%3Fdate+.%0A%7D+ORDER+BY+%3Fdate&contentTypeConstruct=text%2Fturtle&contentTypeSelect=application%2Fsparql-results%2Bjson&endpoint=http%3A%2F%2Flocalhost%3A3030%2Fds&requestMethod=POST&tabTitle=Query&headers=%7B%7D&outputFormat=table)
    ```SPARQL
	PREFIX schema: <https://schema.org/>

	SELECT ?creativeWork ?directorName ?workName ?date
	WHERE {
		VALUES ?directorName { "Nanni Moretti" }
		?director schema:name ?directorName .
		?creativeWork schema:director ?director ;
			schema:name ?workName ;
			schema:datePublished ?date .
	} ORDER BY ?date
    ```
    * [Adminer](http://localhost:8080/?pgsql=postgres&username=postgres&db=imdb&ns=public&sql=SELECT%20%27https%3A%2F%2Fimdb.com%2Ftitle%2F%27%20%7C%7C%20tb.tconst%2C%20nb.primaryname%2C%20tb.primarytitle%2C%20tb.startyear%20%20%0AFROM%20namebasics%20nb%0AJOIN%20director%20d%20ON%20nb.nconst%20%3D%20d.nconst%0AJOIN%20titlebasics%20tb%20ON%20d.tconst%20%3D%20tb.tconst%0AWHERE%20nb.primaryname%20%3D%20%27Nanni%20Moretti%27%0AORDER%20BY%20tb.startYear%20ASC)
    ```SQL
	# An index on nb.primaryname is suggested
	SELECT 'https://imdb.com/title/' || tb.tconst, nb.primaryname, tb.primarytitle, tb.startyear
	FROM namebasics nb
	JOIN director d ON nb.nconst = d.nconst
	JOIN titlebasics tb ON d.tconst = tb.tconst
	WHERE nb.primaryname = 'Nanni Moretti'
	ORDER BY tb.startYear ASC
    ```
---

##  License

This project uses IMDb’s Non-Commercial datasets. Please ensure compliance with [IMDb's Terms of Use](https://datasets.imdbws.com/).
