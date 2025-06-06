.DEFAULT: help

.PHONY: help
help:
	@echo "start - Start the project"
	@echo "stop - Stop the project"
	@echo "init-sqlserver - Init tables on SQL Server DB"
	@echo "populate-sqlserver - Populate tables on SQL Server DB"
	@echo "create-debezium-connector - Create Debezium SQL Server connector to ingest data on kafka"
	@echo "delete-debezium-connector - Delete Debezium SQL Server connector"
	@echo "create-snowflake-connector - Create Snowflake connector to ingest data on Snowflake"
	@echo "delete-snowflake-connector - Delete Snowflake connector"
	@echo "clean-data - Clean up the project data folders"
	@echo "help - Show this help"

.PHONY: start
start:
	mkdir -p data/sqlserver data/kafka data/zoo
	docker compose -f ./infra/docker-compose.yaml up -d

.PHONY: stop
stop:
	docker compose -f ./infra/docker-compose.yaml down --remove-orphans

.PHONY: init-sqlserver
init-sqlserver:
	docker run --rm -v $(PWD)/infra/scripts:/scripts mcr.microsoft.com/mssql-tools /opt/mssql-tools/bin/sqlcmd -S host.docker.internal -U sa -P Password123! -d master -i /scripts/init.sql

.PHONY: populate-sqlserver
populate-sqlserver:
	docker run --rm -v $(PWD)/infra/scripts:/scripts mcr.microsoft.com/mssql-tools /opt/mssql-tools/bin/sqlcmd -S host.docker.internal -U sa -P Password123! -d master -i /scripts/populate.sql

.PHONY: create-debezium-connector
create-debezium-connector:
	curl -v -X POST -H "Content-Type: application/json" --data @./infra/debezium.json http://localhost:8083/connectors | jq

.PHONY: create-snowflake-connector
create-snowflake-connector:
	curl -v -X POST -H "Content-Type: application/json" --data @./infra/snowflake.json http://localhost:8083/connectors | jq

.PHONY: delete-debezium-connector
delete-debezium-connector:
	curl -v -X DELETE http://localhost:8083/connectors/dbz-sqlserver | jq

.PHONY: delete-snowflake-connector
delete-snowflake-connector:
	curl -v -X DELETE http://localhost:8083/connectors/snowflake | jq

.PHONY: clean-data
clean-data:
	@echo "Cleaning up the project data folders..."
	@rm -rf data/sqlserver data/kafa data/zoo