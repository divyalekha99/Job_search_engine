FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY job_scraper.py .

# Force unbuffered stdout and print a clear startup line
# CMD ["bash", "-lc", "echo Starting scraper container... && python -u job_scraper.py"]
