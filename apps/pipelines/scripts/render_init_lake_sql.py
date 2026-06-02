"""Render scripts/init_lake.sql from registered lake table specs."""

from core.schema.ddl import render_init_lake_sql
from lake.schema import ALL_TABLES


def main() -> None:
    """Print the generated lake initialization SQL."""
    print(render_init_lake_sql(ALL_TABLES), end="")


if __name__ == "__main__":
    main()
