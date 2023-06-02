from shared.hooks.postgres.query.add import AddQuery
from shared.hooks.postgres.query.find import FindQuery
from shared.hooks.postgres.query.run import RunQuery
from shared.hooks.postgres.query.update import UpdateQuery
from shared.hooks.postgres.query.copy import CopyQuery


class PgQuery(AddQuery, FindQuery, RunQuery, UpdateQuery, CopyQuery):
    pass
