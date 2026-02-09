from prefect import flow, task
from prefect.artifacts import create_link_artifact


@task
async def my_first_task():
    await create_link_artifact(
        key="irregular-data",
        link="https://nyc3.digitaloceanspaces.com/my-bucket-name/highly_variable_data.csv",
        description="## Highly variable data",
    )


@task
async def my_second_task():
    await create_link_artifact(
        key="irregular-data",
        link="https://nyc3.digitaloceanspaces.com/my-bucket-name/low_pred_data.csv",
        description="# Low prediction accuracy",
    )


@flow
async def my_flow():
    await my_first_task()
    await my_second_task()


if __name__ == "__main__":
    my_flow()
