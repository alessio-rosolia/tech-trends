FROM python:3.10-slim

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

USER appuser

CMD ["python", "-u", "processor.py"]