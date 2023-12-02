import os
import re
from configparser import ConfigParser
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

MigrationDirection = Literal["up", "down"]


class FileCollection(BaseModel):
    name: str
    path: str


class MigrationCommands(BaseModel):
    up: str
    down: str


ALEMBIC_CONFIG = "database/alembic.ini"


class MigrationFile:
    _revision: str
    _sql: str
    _commands: MigrationCommands

    def __init__(self, revision: str) -> None:
        self._revision = revision

    def upgrade(self, wrap_in_trx=False):
        self._get_commands()
        return self._build_commmand(command=self._commands.up, wrap_in_trx=wrap_in_trx)

    def downgrade(self, wrap_in_trx=False):
        self._get_commands()
        return self._build_commmand(command=self._commands.down, wrap_in_trx=wrap_in_trx)

    def _get_commands(self):
        files = self._collect_files()

        file = self._identify_corresponding_sql_file(self._revision, files)
        sql = self._read_sql(file)

        try:
            self._commands = MigrationCommands(
                down=self._extract_command(sql, "down"), up=self._extract_command(sql, "up")
            )

        except Exception as err:
            raise Exception(f'{str(err)} in file "{file.name}"')

    def _build_commmand(self, command: str, wrap_in_trx: bool):
        if wrap_in_trx:
            return f"BEGIN;\n{command}\nCOMMIT;"
        return command

    def _get_path_for_sql_files(self):
        config = ConfigParser()
        config.read(ALEMBIC_CONFIG)

        config.sections()
        script_location = config.get("alembic", option="sql_file_location")
        return Path(script_location).as_posix()

    def _collect_files(self) -> list[FileCollection]:
        path = self._get_path_for_sql_files()
        return [
            FileCollection(name=file.name, path=file.path)
            for file in os.scandir(path)
            if file.is_file() and file.name.endswith(".sql")
        ]

    def _identify_corresponding_sql_file(self, identifier: str, files: list[FileCollection]) -> FileCollection:
        filtered = list(filter(lambda file: file.name.__contains__(identifier), files))

        if len(filtered) > 1:
            raise Exception(f'Multiple SQL files found for identifier "{identifier}"')
        if len(filtered) == 0:
            raise Exception(f'No SQL file found for identifier "{identifier}"')

        return list(filtered)[0]

    def _read_sql(self, file: FileCollection):
        with open(file.path, "r") as f:
            sql = f.read()
        return sql

    def _extract_command(self, sql: str, direction: Literal["up", "down"]):
        if direction == "down":
            identifier = r"\/\*[\s]*DOWN[\s]*\*\/[\S\s\.]*$"
        else:
            identifier = r"^\/\*[\s]*UP[\s]*\*\/[\S\s\.]*\/\*[\s]*DOWN[\s]*\*\/"

        commands_found = re.findall(identifier, sql)

        if len(commands_found) > 1:
            raise Exception(f'Multiple commands found for direction "{direction}"')
        if len(commands_found) == 0:
            return ""

        command = commands_found[0]
        return command
