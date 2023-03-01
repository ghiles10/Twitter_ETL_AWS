# app
init: 
	cd airflow && docker-compose up airflow-init

down:
	docker-compose down

run:
	cd airflow && docker-compose up

# foramts 
REQS=requirements.txt

install: 
	pip install -r $(REQS)

lint: 
	pip install pylint
	pylint --disable=R,C src/

# IaC
tf-init:
	terraform -chdir=./terraform init 

infra-up:
	terraform -chdir=./terraform apply

infra-down:
	terraform -chdir=./terraform destroy

infra-config:
	terraform -chdir=./terraform output
