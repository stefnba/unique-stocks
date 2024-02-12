import re
import typing as t
from io import BytesIO
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.context import Context

CONN_ENV_VAR_PATH = "/opt/spark/env_conn_vars.sh"


class SparkSubmitSHHOperator(SSHOperator):
    """
    Kick off a spark-submit job on a (standalone) Spark cluster through SSH.
    The Spark job file is transfered to the remote cluster.
    """

    template_fields = ("ssh_conn_id", "app_file_name", "app_dir_path", "dataset", "spark_conf")

    ssh_conn_id: str
    app_file_name: str
    spark_app_dir_path: str
    spark_conf: dict
    spark_packages: list[str]
    connections: list[str]
    conn_env_mapping: dict
    dataset: t.Optional[str]

    def __init__(
        self,
        task_id: str,
        ssh_conn_id: str,
        app_file_name: str,
        dataset: t.Optional[str] = None,
        connections: list[str] = [],
        app_dir_path: str = "dags/spark/",
        spark_app_dir_path: str = "/opt/spark/apps/",
        spark_conf: dict = {},
        spark_packages: list[str] = [],
        conn_env_mapping: dict = {},
        **kwargs,
    ):
        self.ssh_conn_id = ssh_conn_id
        self.app_file_name = app_file_name
        self.app_dir_path = app_dir_path
        self.spark_app_dir_path = spark_app_dir_path
        self.spark_conf = spark_conf
        self.spark_packages = spark_packages
        self.connections = connections
        self.conn_env_mapping = conn_env_mapping
        self.dataset = dataset

        super().__init__(
            task_id=task_id,
            ssh_conn_id=ssh_conn_id,
            cmd_timeout=60,
            **kwargs,
        )

    def execute(self, context=Context) -> bytes | str:
        """
        Run spark-submit command on remote cluster.
        """

        hook = SFTPHook(ssh_conn_id=self.ssh_conn_id)

        # combine dir and app_file_name on remote cluster
        self.spark_app_full_path = Path(self.spark_app_dir_path, self.app_file_name)
        # combine dir and app_file_name on local cluster
        self.local_full_path = Path(self.app_dir_path, self.app_file_name)

        self.transfer_conn_env_vars(hook=hook)

        self.copy_app_file(hook)

        self.build_command()
        result = super().execute()

        # delete connections file with secrets
        hook.delete_file(CONN_ENV_VAR_PATH)

        self.delete_app_file(hook)

        return result

    def transfer_conn_env_vars(self, hook: SFTPHook):
        """
        Transfer connections as environment variables to a file on remote cluster that can be sourced when
        using spark-submit.
        """

        conn = self.get_connection_props()
        conn_string = "\n".join([f"export {key}={value}" for key, value in conn.items()])

        self.log.info(
            "Transfering connections as environment variables to remote cluster:"
            f"The following environment variables will be set: {list(conn.keys())}"
        )

        env_vars = BytesIO(conn_string.encode())

        hook.get_conn().putfo(env_vars, CONN_ENV_VAR_PATH)

    def get_connection_props(self) -> dict[str, str]:
        """
        Get all properties for specified connections and return props as dict.

        Name of environment variables is as follows:
        - `CONNECTION_ID__PROPERTY` (for login, host, password, schema, port)
        - `CONNECTION_ID__EXTRA__PROPERTY` (for extra properties)
        """

        env_vars = {}

        for conn_id in self.connections:
            conn = BaseHook.get_connection(conn_id)

            vars = {
                f"{conn_id.upper()}__{prop.upper()}": getattr(conn, prop)
                for prop in ["login", "host", "password", "schema", "port"]
                if getattr(conn, prop)
            }

            # add extra as environment variables
            if conn.extra_dejson:
                extra = {f"{conn_id.upper()}__EXTRA__{key.upper()}": value for key, value in conn.extra_dejson.items()}
                vars.update(extra)

            env_vars.update(vars)

        if self.conn_env_mapping:
            try:
                add_env_vars = {key: env_vars[value] for key, value in self.conn_env_mapping.items()}
                env_vars.update(add_env_vars)
            except KeyError:
                raise AirflowException(
                    "Variable {e} not found in environment variables. "
                    f"Available keys are: {', '.join(list(env_vars.keys()))}"
                )

        return env_vars

    def build_command(self):
        """
        Create all commands and assign to self.command.
        """

        self.log.info(f"Running app '{self.spark_app_full_path}' on remote cluster")

        # Upon container start, all environment variables from Docker are put into /root/environment_variables.sh.
        # This file is sourced before running the spark-submit command
        command = "source /root/environment_variables.sh; \n"

        # source connections as environment variables
        command += f"source {CONN_ENV_VAR_PATH}; \n"

        self.command = command + self.build_submit_command()

    def build_submit_command(self) -> str:
        """
        Build spark-submit command.
        """

        MASTER_URL = "spark://spark-master:7077"
        APP_PATH = self.spark_app_full_path

        command = f"""spark-submit \\\n--deploy-mode client \\\n--master {MASTER_URL} \\\n"""

        # add packages
        if self.spark_packages:
            command += f"""--packages {",".join(self.spark_packages)} \\\n"""

        # add conf
        if self.spark_conf:
            for key, value in self.spark_conf.items():
                command += f"""--conf "{key}={value}" \\\n"""

        if self.dataset:
            command += f"""--conf "spark.datasetPath={self.dataset}" \\\n"""

        command += f"{APP_PATH}"

        return re.sub(r" {2,}", " ", command)

    def copy_app_file(self, hook: SFTPHook):
        """
        Copy application file to remote cluster.
        """

        self.log.info(f"Copying {self.local_full_path} to {self.spark_app_full_path} on remote cluster")

        # create remote directory, will be skipped if already exists
        hook.create_directory(self.spark_app_dir_path)

        hook.store_file(
            local_full_path=self.local_full_path.as_posix(),
            remote_full_path=self.spark_app_full_path.as_posix(),
            confirm=True,
        )

    def delete_app_file(self, hook: SFTPHook):
        """Delete application file from remote cluster after execution."""

        hook.delete_file(self.spark_app_full_path.as_posix())
