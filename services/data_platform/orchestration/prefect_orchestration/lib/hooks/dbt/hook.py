from pathlib import Path
from typing import Optional

from prefect_dbt.cli.commands import DbtCoreOperation


class DbtCoreHook:
    profiles_dir: Path
    project_dir: Path

    def __init__(self, project_dir: str, profiles_dir: str) -> None:
        self.project_dir = Path(project_dir)
        self.profiles_dir = Path(profiles_dir)

    def _select_models(self, models: str | list[str]) -> str:
        if not isinstance(models, list):
            models = [models]

        return " ".join([f"-s {model}" for model in models])

    def run(self, models: Optional[str | list[str]] = None):
        """Execute a dbt Core run command.

        Args:
            models: A list of models to run. If None, all models will be run.
        """

        cmd = "dbt run"

        if models:
            cmd += f" {self._select_models(models)}"

        print(cmd)

        result = DbtCoreOperation(
            commands=[cmd],
            project_dir=self.project_dir,
            profiles_dir=self.profiles_dir,
        ).run()
        return result
