FROM python:3.12-alpine
WORKDIR /app

COPY shared/ shared/
COPY broker/ broker/
COPY miner/ miner/
COPY worker/ worker/

RUN pip install --no-cache-dir pika

ENV PYTHONPATH=/app

CMD ["python", "-m", "worker.worker"]
