from services.hooks.postgres.query.add import AddQuery
from services.hooks.postgres.query.find import FindQuery
from services.hooks.postgres.query.run import RunQuery
from services.hooks.postgres.query.update import UpdateQuery


class PgQuery(AddQuery, FindQuery, RunQuery, UpdateQuery):
    pass
