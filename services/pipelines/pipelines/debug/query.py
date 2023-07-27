# %%
import sys
import os

sys.path.append("..")
os.chdir("..")


# %%

from shared.clients.db.postgres.client import db_client


# db_client.find("""SELECT * FROM "mapping"."figi" """).cursor.scroll(2)

db_client.run("""CREATE TABLE test(id SERIAL PRIMARY KEY, name TEXT)""")

# %%
from pydantic import BaseModel


class Test(BaseModel):
    id: int
    name: str


db_client.add(
    [{"id": 3, "name": "Stefan"}, {"id": 2, "name": "Stefan"}],
    table=("public", "test"),
    column_model=Test,
    conflict="DO_NOTHING",
)
