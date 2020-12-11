# Context

ETL script to transfer data from a CARTO account to a PostgreSQL + PostGIS database using psycopg2 and sqlalchemy

>TIP: Use a docker based environment such as: https://github.com/Mmoncadaisla/geo-toolkit 

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
