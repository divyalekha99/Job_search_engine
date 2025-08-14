FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

# Force unbuffered stdout and print a clear startup line
# CMD ["python", "-u", "consumer.py"]
# CMD ["bash", "-lc", "echo Starting consumer container... && python -u consumer.py"]
