# Always resolve paths relative to this Makefile, regardless of where make is run from
PROJECT_DIR  := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
INCLUDE_DIR  := $(PROJECT_DIR)astro-airflow/include/thelook_ecommerce
AIRFLOW_DIR  := $(PROJECT_DIR)astro-airflow

RSYNC_FLAGS  := -av --delete \
	--exclude='target/' \
	--exclude='dbt_packages/' \
	--exclude='.git/' \
	--exclude='astro-airflow/' \
	--exclude='.claude/' \
	--exclude='logs/'

# ── sync: copy changed files into the container mount (no restart) ─────────────
sync:
	rsync $(RSYNC_FLAGS) $(PROJECT_DIR) $(INCLUDE_DIR)/

# ── deploy: sync + restart Airflow (use after model/macro/yml changes) ─────────
deploy: sync
	cd $(AIRFLOW_DIR) && astro dev restart

# ── watch: auto-sync on every file save (run in a separate terminal tab) ───────
watch:
	@echo "Watching for changes — auto-syncing to include/..."
	@which fswatch > /dev/null || (echo "Installing fswatch..." && brew install fswatch)
	@echo "Initial sync..."
	@rsync $(RSYNC_FLAGS) $(PROJECT_DIR) $(INCLUDE_DIR)/
	@echo "Watching $(PROJECT_DIR) — save any file to trigger sync. Ctrl+C to stop."
	fswatch -o \
		--exclude='.*target.*' \
		--exclude='.*dbt_packages.*' \
		--exclude='.*\.git.*' \
		--exclude='.*astro-airflow.*' \
		--exclude='.*logs.*' \
		$(PROJECT_DIR) | xargs -n1 -I{} rsync $(RSYNC_FLAGS) $(PROJECT_DIR) $(INCLUDE_DIR)/

.PHONY: sync deploy watch
