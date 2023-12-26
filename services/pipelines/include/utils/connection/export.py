from sqlalchemy import create_engine
from sqlalchemy.orm import Session

# retrieving your SQL Alchemy connection
# if you are using Astro CLI this env variable will be set up automatically
sql_alchemy_conn = "postgresql+psycopg2://airflow:airflow@localhost:5418/airflow"

conn_url = f"{sql_alchemy_conn}"

engine = create_engine(conn_url)

# this is a direct query to the metadata database: use at your own risk!
stmt = """SELECT *
        FROM connection;"""

with Session(engine) as session:
    result = session.execute(stmt)
    print(result.all())
