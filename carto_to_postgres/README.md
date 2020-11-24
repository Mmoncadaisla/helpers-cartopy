# Context

ETL script to transfer data from a CARTO account to a PostgreSQL + PostGIS database using psycopg2 and sqlalchemy

# Steps to run the script

1. Install the necessary dependencies

```python
pip install -r requirements.txt
```

2. Open and fill the config.example.json file and change the name to config.json

3. Run the carto_to_postgres.py script 

```python
python carto_to_postgres.py
```
