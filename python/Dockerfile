FROM python:latest

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt 

COPY *.py /
RUN chmod +x /*.py

ENTRYPOINT ["/main.py"]
