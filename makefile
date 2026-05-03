TEMP_DIR := tmp_codegen

# IMPORTANT: Ensure you use TABs for indentation, not spaces!
nessie-merge:
	bash ./scripts/nessie_merge_branch.sh

nessie-cli:
	docker compose run --rm nessie-cli --uri "http://nessie:19120/iceberg"

gc-sweep:
	@bash ./scripts/nessie_maintenance.sh

airflow-cli:
	docker compose run --rm airflow-cli bash

yaml:
	@# Usage: make yaml m=<model_name>
	@# 1. Create temp directory if it doesn't exist
	@mkdir -p $(TEMP_DIR)
	
	@# 2. Get current timestamp
	$(eval TIMESTAMP := $(shell date '+%Y%m%d_%H%M%S'))
	
	@echo "⏳ Generating YAML for model '$(m)'..."
	
	@# 3. Run dbt macro and save to file
	@dbt run-operation generate_model_yaml --args '{"model_names": ["$(m)"]}' > $(TEMP_DIR)/$(TIMESTAMP)_$(m).yml

test:
	@if [ ! -n "$(m)" ]; then \
		dbt test --select $(m); \
	else \
		dbt test; \
	fi

run:
	@if [ -z "$(m)" ]; then \
		echo "Missing model argument. Usage: make run m=stg_companies_listing"; \
	else \
		dbt run --select $(m); \
	fi

op:
	@if [ -z "$(m)" ]; then \
		echo "Error: Missing macro name. Usage: make op m=cleanup_elementary_logs a=days:7"; \
	else \
		if [ -z "$(a)" ]; then \
			echo "🔧 Running macro: $(m) (No arguments)..."; \
			dbt run-operation $(m); \
		else \
			echo "Running macro: $(m) with arguments: {$(a)}..."; \
			dbt run-operation $(m) --args '{$(a)}'; \
		fi \
	fi
