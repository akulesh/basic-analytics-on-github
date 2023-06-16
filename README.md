# GitHub Analytics
## Tools
- Python
- PySpark
- Prefect
- Terraform
- PostgreSQL

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

## Run Dashboard
```
PYTHONPATH=. streamlit run src/app.py
```

# References:
- https://docs.streamlit.io/library/api-reference