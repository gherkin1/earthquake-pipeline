@echo off
cd /d "%~dp0"
docker compose run --rm app >> cron.log 2>&1
