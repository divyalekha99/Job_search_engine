## Job search engine

## 1. Clone the repo
git clone https://github.com/your-handle/automated-job-finder.git
cd automated-job-finder

## 2. (Optional) adjust environment variables
cp .env.example .env           # edit if you need custom ports, creds, etc.

## 3. Spin everything up
docker compose \
  -f docker-compose.yml \
  -f docker-compose-airflow.yml \
  up -d --build

##4. Shut down
docker compose \
  -f docker-compose.yml \
  -f docker-compose-airflow.yml \
  down -v


