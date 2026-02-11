FROM python:latest

COPY etl.py /
RUN pip install psycopg && chmod +x /etl.py

ENTRYPOINT ["/etl.py"]
