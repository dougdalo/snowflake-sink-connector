.DEFAULT: help

.PHONY: help
help:
	@echo "start - Start the project"
	@echo "stop - Stop the project"
	@echo "init-sqlserver - Init tables on SQL Server DB"
	@echo "populate-sqlserver - Populate tables on SQL Server DB"
	@echo "clean-sqlserver - Clean tables on SQL Server DB"
	@echo "create-debezium-connector - Create Debezium SQL Server connector to ingest data on kafka"
	@echo "delete-debezium-connector - Delete Debezium SQL Server connector"
	@echo "create-snowflake-connector - Create Snowflake connector to ingest data on Snowflake"
	@echo "delete-snowflake-connector - Delete Snowflake connector"
	@echo "clean-data - Clean up the project data folders"
	@echo "help - Show this help"

.PHONY: start
start:
	@mkdir -p data/sqlserver data/kafka data/zoo
	@echo "Starting the Docker Compose..."
	@docker compose -f ./infra/docker-compose.yaml up -d
	@kubectl create ns strimzi || true
	kubectl apply -f ./infra/k8s/strimzi-cluster-operator-0.43.0.yaml -n strimzi || true
	@echo "Waiting for Strimzi Cluster Operator to be ready using k8s..."
	@kubectl wait --for=condition=Ready pod -l name=strimzi-cluster-operator -n strimzi --timeout=120s
	@echo "Installing Strimzi Kafka Connect Cluster using K8s..."
	@kubectl apply -f ./infra/k8s/kconnect_cluster.yaml

.PHONY: stop
stop:
	docker compose -f ./infra/docker-compose.yaml down --remove-orphans

.PHONY: init-sqlserver
init-sqlserver:
	docker run --rm -v $(PWD)/infra/scripts:/scripts mcr.microsoft.com/mssql-tools /opt/mssql-tools/bin/sqlcmd -S host.docker.internal -U sa -P Password123! -d master -i /scripts/init.sql

.PHONY: populate-sqlserver
populate-sqlserver:
	docker run --rm -v $(PWD)/infra/scripts:/scripts mcr.microsoft.com/mssql-tools /opt/mssql-tools/bin/sqlcmd -S host.docker.internal -U sa -P Password123! -d master -i /scripts/populate.sql

.PHONY: clean-sqlserver
clean-sqlserver:
	docker run --rm -v $(PWD)/infra/scripts:/scripts mcr.microsoft.com/mssql-tools /opt/mssql-tools/bin/sqlcmd -S host.docker.internal -U sa -P Password123! -d master -i /scripts/delete_all.sql


.PHONY: create-debezium-connector
create-debezium-connector:
	kubectl apply -f ./infra/k8s/connectors/source-sqlserver.yaml -n strimzi

.PHONY: create-snowflake-connector
create-snowflake-connector:
	kubectl apply -f ./infra/k8s/connectors/sink-snowflake.yaml -n strimzi

.PHONY: delete-debezium-connector
delete-debezium-connector:
	kubectl delete -f ./infra/k8s/connectors/source-sqlserver.yaml -n strimzi

.PHONY: delete-snowflake-connector
delete-snowflake-connector:
	kubectl delete -f ./infra/k8s/connectors/sink-snowflake.yaml -n strimzi

.PHONY: clean-data
clean-data:
	@echo "Cleaning up the project data folders..."
	@rm -rf infra/data/*