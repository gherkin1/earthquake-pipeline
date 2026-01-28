# Earthquake Pipeline (USGS → CSV + MySQL)

A small Dockerized Python pipeline that pulls recent earthquake events from the USGS API, saves them to CSV, and uploads them into a MySQL database.

## What it does
- Queries USGS Earthquake API (GeoJSON)
- Transforms the data into a tabular format (date, lat, lon, depth, place, id)
- Writes CSV outputs to `./data`
- Appends records into MySQL (via SQLAlchemy)

## Tech stack
- Python (pandas, requests)
- MySQL 8
- Docker + Docker Compose
- (Optional) Windows Task Scheduler for minutely runs

---

## Project structure
- `main.py` – pipeline logic (fetch + transform + call upload functions)
- `config.py` – helper functions (CSV + MySQL upload)
- `docker-compose.yml` – runs MySQL and the app
- `.env.example` – example environment variables (copy to `.env`)

---

## Prerequisites
- Docker Desktop (Windows/macOS) or Docker Engine (Linux)
- Docker Compose (`docker compose`)

---

## Setup

### 1) Clone repo
```bash
git clone https://github.com/gherkin1/earthquake-pipeline.git
cd earthquake-pipeline
