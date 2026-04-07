TEMP_DIR := tmp_codegen


# Đảm bảo dùng thụt lề bằng phím TAB, không dùng dấu cách nhé sếp!
nessie-merge:
	bash ./scripts/nessie_merge_branch.sh

nessie-cli:
	docker compose run --rm nessie-cli --uri "http://nessie:19120/iceberg"

gc-sweep:
	@bash ./scripts/nessie_maintenance.sh

yaml:
	@# Cách dùng: make yaml m=<tên_model>
	@# 1. Create temp directory if it doesn't exist
	@mkdir -p $(TEMP_DIR)
	
	@# 2. Lấy ngày giờ hiện tại 
	$(eval TIMESTAMP := $(shell date '+%Y%m%d_%H%M%S'))
	
	@echo "⏳ đang sinh code cho model '$(m)'..."
	
	@# 3. Chạy lệnh và lưu file
	@dbt run-operation generate_model_yaml --args '{"model_names": ["$(m)"]}' > $(TEMP_DIR)/$(TIMESTAMP)_$(m).yml
	
# source:
# 	@# Cách dùng: make source s=<tên_schema>
# 	@mkdir -p $(TEMP_DIR)
	
# 	$(eval TIMESTAMP := $(shell date '+%Y%m%d_%H%M%S'))
	
# 	@echo "⏳ Đang quét Database schema '$(s)' từ Nessie/Trino..."
	
# 	@# 4. Chạy lệnh dbt và lưu vào thư mục tmp_codegen
# 	@dbt run-operation generate_source --args '{"schema_name": "$(s)", "generate_columns": true}' > $(TEMP_DIR)/$(TIMESTAMP)_source_$(s).yml


test:
	@if [ ! -n "$(m)" ]; then \
		dbt test --select $(m); \
	else \
		dbt test; \
	fi

run:
	@if [ -z "$(m)" ]; then \
		echo "⚠️ empty model arg, eg: make run m=stg_companies_listing"; \
	else \
		dbt run --select $(m); \
	fi

op:
	@if [ -z "$(m)" ]; then \
		echo "⚠️  Lỗi: Thiếu tên macro. Ví dụ: make op m=cleanup_elementary_logs a=days:7"; \
	else \
		if [ -z "$(a)" ]; then \
			echo "🔧 Đang chạy macro: $(m) (Không tham số)..."; \
			dbt run-operation $(m); \
		else \
			echo "🔧 Đang chạy macro: $(m) với tham số: {$(a)}..."; \
			dbt run-operation $(m) --args '{$(a)}'; \
		fi \
	fi
