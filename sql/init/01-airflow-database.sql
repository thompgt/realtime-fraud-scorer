-- Postgres runs this once, on first initialisation of an empty data volume.
-- The `fraud` database and role come from POSTGRES_* env vars; Airflow needs
-- its own database and role alongside them.
CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
