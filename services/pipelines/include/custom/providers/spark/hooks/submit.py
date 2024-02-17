import re
import typing as t
from io import BytesIO
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from utils.file.unzip import compress_file

ENV_PATH = "/opt/spark/env_conn_vars.sh"


class SparkSSHSubmitHook(BaseHook):
    """
    Kick off a spark-submit job on a (standalone) Spark cluster through SSH.
    The Spark job file is transfered to the remote cluster.
    """

    ssh_conn_id: str
    ssh_hook: SSHHook
    sftp_hook: SFTPHook

    def __init__(self, ssh_conn_id: str, cmd_timeout=60 * 10):
        self.ssh_conn_id = ssh_conn_id

        self.ssh_hook = SSHHook(ssh_conn_id, cmd_timeout=cmd_timeout)
        self.sftp_hook = SFTPHook(ssh_conn_id)

    def submit(
        self,
        app_file_name: str,
        local_app_dir_path: str = "dags/spark/",
        remote_app_dir_path: str = "/opt/spark/apps/",
        master: t.Optional[str] = "spark://spark-master:7077",
        packages: t.Optional[list[str]] = None,
        conf: t.Optional[dict] = None,
        dataset: t.Optional[str] = None,
        py_files: t.Optional[list[str]] = None,
        py_files_dir: str = "spark_utils",
        env_vars: t.Optional[dict[str, str]] = None,
        deploy_mode: t.Optional[str] = "client",
    ):
        env = None
        if env_vars:
            env = env_vars

        # transfer app file
        self._transfer_app_file(
            local_app_dir_path=local_app_dir_path,
            app_file_name=app_file_name,
            remote_app_dir_path=remote_app_dir_path,
        )

        # transfer additional python scripts
        py_files_path = self._transfer_python_scripts(
            files=py_files,
            py_files_dir=py_files_dir,
            local_app_dir_path=local_app_dir_path,
            remote_app_dir_path=remote_app_dir_path,
        )

        cmd = self._build_submit_cmd(
            app_file_name=app_file_name,
            remote_app_dir_path=remote_app_dir_path,
            master=master,
            packages=packages,
            conf=conf,
            dataset=dataset,
            deploy_mode=deploy_mode,
            py_files=[py_files_path] if py_files_path else None,
        )

        # run spark-submit
        result = self._run_ssh_cmd(cmd=cmd, env=env)

        # delete app file
        self.remove_app_file(app_file_name=app_file_name, remote_app_dir_path=remote_app_dir_path)

        return result

    def _transfer_python_scripts(
        self,
        files: t.Optional[list[str]],
        py_files_dir: str,
        local_app_dir_path: str,
        remote_app_dir_path: str,
    ) -> None | str:
        """
        Transfer additional python scripts to remove spark cluster and zip those file in preparation for submit
        argument `--py-files`.

        Args:
            files (list[str]): List of file paths relative to `py_files_dir`.
            py_files_dir (str): Base dir in which `files` are located.
            local_app_dir_path (str): Path to dir on Airflow with pyspark scripts.
            remote_app_dir_path (str): Path to spark apps on remote cluster.
        """

        if not files:
            return None

        if "__init__.py" not in files:
            # add __init__.py to make it a package
            files = [*files, "__init__.py"]

        # transfer all files
        zip_path = compress_file(
            files=[(Path(py_files_dir) / file).as_posix() for file in files],
            base_dir=Path(local_app_dir_path).resolve().as_posix(),
        )

        # remote zip path
        remote_zip_path = Path(remote_app_dir_path, py_files_dir + ".zip").as_posix()

        # transfer zip file
        try:
            self.sftp_hook.delete_directory(remote_zip_path)
        except FileNotFoundError:
            pass

        self.sftp_hook.store_file(local_full_path=zip_path, remote_full_path=remote_zip_path)

        return remote_zip_path

    def get_env_from_conn(self, connections: list[str], mapping: t.Optional[dict[str, str]] = None) -> dict[str, str]:
        """
        Get all properties for specified connections and return props as dict.

        Name of environment variables is as follows:
        - `CONNECTION_ID__PROPERTY` (for login, host, password, schema, port)
        - `CONNECTION_ID__EXTRA__PROPERTY` (for extra properties)
        """

        env_vars = {}

        for conn_id in connections:
            conn = self.get_connection(conn_id)

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

        if mapping:
            try:
                add_env_vars = {key: env_vars[value] for key, value in mapping.items()}
                env_vars.update(add_env_vars)
            except KeyError:
                raise AirflowException(
                    "Variable {e} not found in environment variables. "
                    f"Available keys are: {', '.join(list(env_vars.keys()))}"
                )

        return env_vars

    def _run_ssh_cmd(self, cmd: str, env: t.Optional[dict[str, str]] = None):
        """"""

        if env:
            # copy env to remote and source
            self.log.info(f"Setting environment variables: {list(env.keys())}")
            env_vars = "\n".join([f"export {key}={value}" for key, value in env.items()])
            self.sftp_hook.get_conn().putfo(BytesIO(env_vars.encode()), ENV_PATH)
            cmd = f"source {ENV_PATH};\n{cmd}"

        # Upon container start, all environment variables from Docker are put into /root/environment_variables.sh.
        # This file is sourced before running the spark-submit command
        cmd = "\nsource /root/environment_variables.sh;\n" + cmd

        with self.ssh_hook.get_conn() as ssh_client:
            result = self.ssh_hook.exec_ssh_client_command(ssh_client, command=cmd, get_pty=True, environment=None)

        if env:
            # delete env file
            self.sftp_hook.delete_file(ENV_PATH)

        return result

    def _build_submit_cmd(
        self,
        app_file_name: str,
        remote_app_dir_path: str,
        master: t.Optional[str] = None,
        packages: t.Optional[list[str]] = None,
        conf: t.Optional[dict] = None,
        dataset: t.Optional[str] = None,
        py_files: t.Optional[list[str]] = None,
        deploy_mode: t.Optional[str] = "client",
    ) -> str:
        """
        Build spark-submit with given parameters and return the command.
        """

        cmd = f"""spark-submit \\\n--deploy-mode {deploy_mode} \\\n--master {master} \\\n"""

        # add packages
        if packages:
            cmd += f"""--packages {",".join(packages)} \\\n"""

        # add conf
        if conf:
            for key, value in conf.items():
                cmd += f"""--conf "{key}={value}" \\\n"""

        # add py_files
        if py_files:
            cmd += f"""--py-files {",".join(py_files)} \\\n"""

        # add dataset
        if dataset:
            cmd += f"""--conf "spark.datasetPath={dataset}" \\\n"""

        cmd += f"""{Path(remote_app_dir_path, app_file_name).as_posix()}"""

        return re.sub(r" {2,}", " ", cmd)

    def _transfer_app_file(self, local_app_dir_path: str, app_file_name: str, remote_app_dir_path: str):
        """
        Copy application file to remote cluster.
        """

        local_full_path = Path(local_app_dir_path, app_file_name).as_posix()
        spark_app_full_path = Path(remote_app_dir_path, app_file_name).as_posix()

        self.log.info(f"Copying '{local_full_path}' to '{spark_app_full_path}' on remote cluster")

        # create remote directory, will be skipped if already exists
        self.sftp_hook.create_directory(remote_app_dir_path)

        self.sftp_hook.store_file(local_full_path=local_full_path, remote_full_path=spark_app_full_path, confirm=True)

    def remove_app_file(self, app_file_name: str, remote_app_dir_path: str):
        """
        Delete application file from remote cluster after execution.
        """

        spark_app_full_path = Path(remote_app_dir_path, app_file_name).as_posix()

        self.log.info(f"Deleting app file '{spark_app_full_path}' from remote cluster")

        self.sftp_hook.delete_file(spark_app_full_path)
