# GitHub Analytics
## Tools
- Docker
- Docker Compose
- Python
- Prefect
- Terraform
- PostgreSQL
- Pandas
- Streamlit

## Docker
```
docker-compose up --build

docker exec -it <CONTAINER_ID> <CMD>

docker-compose down --volumes
```

## PostgreSQL
```
psql --username=postgres --dbname=postgres
```

## Virtual environment setup

Install Miniconda:
```
FILE_NAME=Miniconda3-py310_23.1.0-1-Linux-x86_64.sh

wget -P ./tmp https://repo.anaconda.com/miniconda/$FILE_NAME
bash ./tmp/$FILE_NAME
```

Reference:
- https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html


Create a virtual environment:
```
EVN_NAME=github
PYTHON_VERSION=3.10

conda create -n $EVN_NAME python=$PYTHON_VERSION
conda activate $EVN_NAME
pip install -r requirements.txt
```

Remove the virtual environment:
```
conda deactivate
conda env remove --name=$EVN_NAME
```

## Run ETL
```
PYTHONPATH=. python src/etl/main.py \
    --skip_extraction
```
docker run --network=basic-analytics-on-github_prefect -it basic-analytics-on-github_etl bash
prefect config set PREFECT_API_URL=http://server:4200/api
PYTHONPATH=. python src/etl/deploy.py

## Run Dashboard
```
PYTHONPATH=. streamlit run src/app.py
```

# References:
- https://docs.streamlit.io/library/api-reference
- https://geshan.com.np/blog/2021/12/docker-postgres/
- https://www.datacamp.com/tutorial/wordcloud-python
- https://docs.docker.com/compose/
- https://docs.docker.com/compose/startup-order/
- https://docs.docker.com/compose/compose-file/05-services/#depends_on
- https://docs.prefect.io/2.10.16/concepts/deployments/
- https://github.com/flavienbwk/prefect-docker-compose/blob/main/client/app/weather.py
- https://github.com/rpeden/prefect-docker-compose/blob/main/docker-compose.yml
- https://docs.prefect.io/2.10.16/concepts/deployments/
- https://gist.github.com/rxaviers/7360908
- https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html