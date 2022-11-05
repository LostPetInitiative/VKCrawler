FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY run.py /app

CMD python3 -u run.py