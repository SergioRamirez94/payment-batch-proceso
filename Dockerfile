FROM python:3.11.7-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./patches/typeconv.py /usr/local/lib/python3.11/site-packages/immudb/

COPY ./src /app/src

CMD ["python", "./src/main.py"]