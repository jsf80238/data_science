FROM python:3 AS base
#FROM python:3.11-slim-bookworm AS base
WORKDIR /app

RUN apt-get update && apt-get install -y curl redis-server

COPY . .
RUN pip install -r requirements.txt

EXPOSE 5000

CMD ["bash", "start-all.sh"]
