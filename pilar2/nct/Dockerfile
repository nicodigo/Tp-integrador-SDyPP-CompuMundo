FROM python:3.12-alpine
WORKDIR /app

RUN apk add --no-cache curl

COPY shared/ shared/
COPY broker/ broker/
COPY storage/ storage/
COPY nct/ nct/

RUN pip install --no-cache-dir redis pika

ENV PYTHONPATH=/app

CMD ["python", "-m", "nct.nct"]
