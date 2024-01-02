from pathlib import Path

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.context import Context


class SparkSubmitSHHOperator(SSHOperator):
    """
    Kick off a spark-submit job on a (standalone) Spark cluster through SSH.
    The Spark job file is transfered to the remote cluster.
    """

    template_fields = (
        "ssh_conn_id",
        "app_file_name",
        "app_dir_path",
    )

    ssh_conn_id: str
    app_file_name: str
    spark_app_dir_path: str

    def __init__(
        self,
        task_id: str,
        ssh_conn_id: str,
        app_file_name: str,
        app_dir_path: str = "./dags/spark/",
        spark_app_dir_path: str = "/opt/spark/apps/",
        **kwargs,
    ):
        """_summary_

        Args:
            task_id (str): _description_
            ssh_conn_id (str): _description_
            app_file_name (str): _description_
            app_dir_path (str, optional): _description_. Defaults to "./dags/spark/".
            spark_app_dir_path (str, optional): _description_. Defaults to "/opt/spark/apps/".
        """
        self.ssh_conn_id = ssh_conn_id
        self.app_file_name = app_file_name
        self.app_dir_path = app_dir_path
        self.spark_app_dir_path = spark_app_dir_path

        super().__init__(
            task_id=task_id,
            ssh_conn_id=ssh_conn_id,
            **kwargs,
        )

    def execute(self, context=Context) -> bytes | str:
        """Run spark-submit command on remote cluster."""

        hook = SFTPHook(ssh_conn_id=self.ssh_conn_id)

        # combine dir and app_file_name on remote cluster
        self.spark_app_full_path = Path(self.spark_app_dir_path, self.app_file_name)
        # combine dir and app_file_name on local cluster
        self.local_full_path = Path(self.app_dir_path, self.app_file_name)

        self.copy_file(hook)

        self.command = (
            f"spark-submit --deploy-mode client --master spark://spark-master:7077 {self.spark_app_full_path}"
        )

        result = super().execute()

        self.delete_file(hook)

        return result

    def copy_file(self, hook: SFTPHook):
        """Copy application file to remote cluster."""

        hook.store_file(
            local_full_path=self.local_full_path.as_posix(),
            remote_full_path=self.spark_app_full_path.as_posix(),
            confirm=True,
        )

    def delete_file(self, hook: SFTPHook):
        """Delete application file from remote cluster after execution."""

        hook.delete_file(self.spark_app_full_path.as_posix())
