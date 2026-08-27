FROM python:3.14-slim

WORKDIR /worker

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY worker .

ENV PYTHONPATH=/worker:/shared

CMD ["python", "main.py"]
