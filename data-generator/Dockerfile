FROM python:3.9-slim

WORKDIR /app

COPY ./data-generator/requirements.txt /app/

COPY ./data-generator/producer.py /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "producer.py"]