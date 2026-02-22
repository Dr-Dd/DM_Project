FROM python:latest

COPY etl.py /
RUN pip install psycopg[binary] rapidgzip && chmod +x /etl.py

ENTRYPOINT ["/etl.py"]
