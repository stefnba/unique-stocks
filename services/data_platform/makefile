#!make
include .env

# Docker
docker-up:
	docker compose -f docker/dev/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans
	docker rm -f mc