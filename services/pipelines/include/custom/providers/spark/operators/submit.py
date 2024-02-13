import typing as t

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from custom.providers.spark.hooks.submit import SparkSSHSubmitHook

CONN_ENV_VAR_PATH = "/opt/spark/env_conn_vars.sh"


class SparkSubmitSHHOperator(BaseOperator):
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
        py_files: t.Optional[list[str]] = None,
        py_files_dir: str = "spark_utils",
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
        self.py_files_dir = py_files_dir
        self.py_files = py_files

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context=Context):
        """
        Run spark-submit command on remote cluster.
        """

        hook = SparkSSHSubmitHook(ssh_conn_id=self.ssh_conn_id)

        env_vars = hook.get_env_from_conn(self.connections, self.conn_env_mapping)

        hook.submit(
            app_file_name=self.app_file_name,
            remote_app_dir_path=self.spark_app_dir_path,
            local_app_dir_path=self.app_dir_path,
            dataset=self.dataset,
            conf=self.spark_conf,
            packages=self.spark_packages,
            py_files=self.py_files,
            py_files_dir=self.py_files_dir,
            env_vars=env_vars,
        )
