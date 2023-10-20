# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["fundamental"],
)
def fundamental_groupB():
    TriggerDagRunOperator(
        wait_for_completion=False,
        trigger_dag_id="fundamental",
        task_id="trigger",
        conf={
            "delta_table_mode": "overwrite",
            "exchanges": [
                "SW",
                "LSE",
            ],
            "security_types": [
                "common_stock",
                "preferred_stock",
            ],
        },
    )


dag_object = fundamental_groupB()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"
    variables = "testing/connections/variables.yaml"

    dag_object.test(
        conn_file_path=connections,
        variable_file_path=variables,
    )
