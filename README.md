# GitHub Analytics
This repository contains a python application that offers global analytics of GitHub repositories. It utilizes  the GitHub RESTful API for data extraction and allows you to explore repository metrics and trends easily 🚀

Link to the app: `TODO`

The following languages are supported (primary repository language):
- Python
- Jupyter Notebook
- HTML
- Markdown
- Shell
- Java
- JavaScript
- TypeScript
- C
- C++
- C#
- Go
- Rust


The main components:
- Docker (with Docker Compose) - containerization and easy deployment of the application
- Prefect - data flow orchestration
- Postgres - data storage
- Pandas - data processing
- Streamlit - the interactive dashboard interface
- (Optional) Terraform - creating infrastructure on AWS

# Quick start
## Docker
Run from the repo root directory to fire up all components:
```
docker-compose up --build
```

Stop the application:
```
docker-compose down --volumes
```

## Conda dev environment
Create a virtual environment:
```
EVN_NAME=github
PYTHON_VERSION=3.10

conda create -n $EVN_NAME python=$PYTHON_VERSION
conda activate $EVN_NAME
pip install -r requirements-dev.txt
```

Remove the virtual environment:
```
conda deactivate
conda env remove --name=$EVN_NAME
```

### Run Dashboard
```
source .env
export POSTGRES_HOST=0.0.0.0
PYTHONPATH=. streamlit run src/dashboard/app.py
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
- Pre-commit hooks: https://pre-commit.com/
- Hooks: https://pre-commit.com/hooks.html
- Black: https://black.readthedocs.io/en/stable/guides/using_black_with_other_tools.html