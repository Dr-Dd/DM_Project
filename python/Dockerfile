FROM python:latest

COPY *.py /
RUN pip install psycopg[binary] rapidgzip && chmod +x /*.py

ENTRYPOINT ["/etl.py"]
