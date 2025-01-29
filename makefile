.PHONY: 

all: prod_py cons_py

up:
	docker compose up -d

prod_py:
	python3 src/producer.py

cons_py:
	python3 src/consumer.py

bash:
	docker compose exec -it kafka bash

redo_py:
	rm cpu_data.json

down:
	docker compose down