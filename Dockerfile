FROM python:3.11-slim

WORKDIR /app

RUN useradd -m workeruser

RUN chown -R workeruser:workeruser /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src

USER workeruser

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]