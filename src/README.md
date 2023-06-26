# Github API scrapping
```
conda activate github
```


## Orchestration with Prefect
```
prefect project init

prefect orion start

prefect deployment build ./src/data_extraction.py:extract -n 'github-api-scrapping'
prefect deployment apply extract-deployment.yaml

prefect deployment build ./src/data_transformation.py:transform -n 'github-data-transformation'
prefect deployment apply transform-deployment.yaml

PREFECT_LOGGING_EXTRA_LOGGERS=my_logger prefect agent start -p 'default-agent-pool'
```


## Download repositories metadata
```
PREFECT_LOGGING_EXTRA_LOGGERS=my_logger 

PYTHONPATH=. python src/etl/extractors/repo_metadata.py \
    --start_date=2012-01-01\
    --end_date=2012-01-07\
    --languages="python"\
    --output_dir="/Users/a_kulesh/Workspace/education/pet-projects/basic-analytics-on-github/tmp/data/raw/repositories"\
    --overwrite_existed_files

PYTHONPATH=. python src/etl/flows.py \
    --start_date=2020-01-01\
    --end_date=2020-01-01\
    --languages="python"\
    --source_dir="/Users/a_kulesh/Workspace/education/pet-projects/basic-analytics-on-github/tmp/data/raw/repositories"\
    --db_host="0.0.0.0"\
    --skip_extraction
```

## Transform repositories metadata
```
PYTHONPATH=. python src/data_transformation.py \
    --input_dir="/Users/a_kulesh/Workspace/education/pet-projects/basic-analytics-on-github/tmp/data/raw/repositories"\
    --output_dir="/Users/a_kulesh/Workspace/education/pet-projects/basic-analytics-on-github/tmp/data/processed/repositories"\
    --language="*"\
    --limit=10000
```

## Statistics
```
du -sh sandbox/github/data/raw/*
```

## Terraform
### SSH access keys
```
cd terraform
mkdir -p .ssh/ && ssh-keygen -f .ssh/id_rsa
chmod 400 .ssh/id_rsa
```

### Set up secret variables
```
export AWS_ACCESS_KEY_ID=<>
export AWS_SECRET_ACCESS_KEY=<>
export AWS_SESSION_TOKEN=<>
export AWS_DEFAULT_REGION=us-east-1
```

### Commands
```
cd terraform
terraform init

terraform apply -var="instance_type=t4g.small" -var="instance_ami=ami-0750be70a912aa1e9"
terraform apply

terraform destroy
```

## EC2

### Load data to server:
on server: mkdir -p ~/sandbox/github
scp -r ./etl/*.py ./etl/README.md idl-dev-sandbox:~/sandbox/github/etl
scp .env requirements.txt README.md idl-dev-sandbox:~/sandbox/github

### Load data from server:
scp -r idl-dev-sandbox:~/sandbox/github/data/raw/repositories ./tmp

## Reference:
- https://aws.amazon.com/ec2/pricing/on-demand/
- https://tmuxcheatsheet.com/