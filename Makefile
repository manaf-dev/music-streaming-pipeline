TF_DIR := terraform/main
FILE   ?= streams1.csv

.PHONY: help init apply upload

help: ## Show targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

init: ## terraform init (set TF_STATE_BUCKET and AWS_REGION)
	@test -n "$(TF_STATE_BUCKET)" || (echo "Set TF_STATE_BUCKET" && exit 1)
	@test -n "$(AWS_REGION)" || (echo "Set AWS_REGION" && exit 1)
	cd $(TF_DIR) && terraform init \
		-backend-config="bucket=$(TF_STATE_BUCKET)" \
		-backend-config="region=$(AWS_REGION)"

apply: ## terraform apply using terraform.tfvars
	cd $(TF_DIR) && terraform apply -var-file=terraform.tfvars

upload: ## Upload streams CSV — make upload FILE=streams1.csv (uses terraform.tfvars bucket_name)
	bash scripts/upload_sample_data.sh $(FILE)
