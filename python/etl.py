# Data configuration file
import gzip
import psycopg2
from psycopg2.extras import execute_batch
import csv

# Global variables for data paths and files
data_path = "data"

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
conn_params = {
    'host': 'localhost',
    'database': 'your_db',
    'user': 'your_user',
    'password': 'your_password',
    'port': 5432
}

def load_data():
    for f in data_files:

