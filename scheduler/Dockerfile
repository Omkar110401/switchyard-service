FROM python:3.14-slim

WORKDIR /scheduler

COPY scheduler/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY scheduler .

ENV PYTHONPATH=/scheduler:/shared

CMD ["python", "main.py"]
