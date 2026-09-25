FROM python:3.14-slim

WORKDIR /app

COPY scheduler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY scheduler ./scheduler

ENV PYTHONPATH=/app

CMD ["python", "-m", "scheduler.main"]
