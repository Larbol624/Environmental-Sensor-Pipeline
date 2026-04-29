help:
	@echo +----------------------------------------------------------------------------+
	@echo Environmental Sensor Data Pipeline
	@echo +----------------------------------------------------------------------------+
	@echo   make run              		Start only the core pipeline
	@echo   make build                  Build all the containers needed for the pipeline						   
	@echo   make dev              		Start pipeline + dev tools (pgAdmin, Jupyter)  
	@echo   make down             		Stop all containers							   
	@echo   make build-tests      		Build the test containers					   
	@echo   make test-spark       		Run Spark unit tests						   
	@echo   make test-integration 		Run integration tests						   
	@echo   make logs             		Tail logs from all containers				   
	@echo   make clean            		Stop containers and remove volumes			   
	@echo   make network          		Create the Docker network (run once)
	@echo   make setup					Create the network and .env file		   
	@echo +----------------------------------------------------------------------------+

test-spark:
	docker compose -f docker-compose.test.yml up --abort-on-container-exit

test-integration:
	docker compose -f docker-compose.integration.yml up --abort-on-container-exit

build-tests:
	docker compose -f docker-compose.integration.yml build
	docker compose -f docker-compose.test.yml build

network:
	docker network create env_network || @echo Network already exists

clean:
	docker compose down -v

run: network
	docker compose -f docker-compose.yml up -d

dev: network
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

build:
	docker compose build

setup: network
	@echo "Setting up environment..."
	@if not exist configs\postgres\.env copy configs\postgres\example_postgres_env.txt configs\postgres\.env
	@echo "Done! Edit configs/postgres/.env with your values before running make run"