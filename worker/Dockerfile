FROM python:3.14-slim

WORKDIR /app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY worker ./worker

ENV PYTHONPATH=/app

CMD ["python", "-m", "worker.main"]
