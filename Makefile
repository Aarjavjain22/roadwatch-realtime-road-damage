.PHONY: build up spark ps logs down clean terraform-init terraform-apply

build:
	docker compose build

up:
	docker compose up -d

spark:
	docker compose --profile spark up -d spark

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

down:
	docker compose --profile spark down

clean:
	docker compose --profile spark down -v

terraform-init:
	cd terraform && terraform init

terraform-apply:
	cd terraform && terraform apply
