# GitHub Analytics Dashboard

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
PYTHONPATH=. streamlit run src/dashboard/app.py
```

# References:
- https://docs.streamlit.io/library/api-reference