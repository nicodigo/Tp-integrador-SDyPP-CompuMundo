FROM python:3.12-alpine
WORKDIR /app

COPY shared/ shared/
COPY broker/ broker/
COPY pool/ pool/

RUN pip install --no-cache-dir pika fastapi uvicorn

ENV PYTHONPATH=/app

CMD ["python", "-m", "pool.pool"]
