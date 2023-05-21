# %%
exchange_code = "OTCQX"


def ingest(exchange: str):
    from dags.security.eod_historical_data.jobs import ASSET_SOURCE, ingest
    from dags.security.path import SecurityPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        func=lambda: ingest(exchange),
        commit_path=SecurityPath.raw(source=ASSET_SOURCE, bin=exchange_code, file_type="json"),
    )


def transform(file_path: str):
    from dags.security.eod_historical_data.jobs import ASSET_SOURCE, transform
    from dags.security.path import SecurityPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: transform(data),
        commit_path=SecurityPath.processed(source=ASSET_SOURCE, bin=exchange_code),
    )


# %%

path = ingest(exchange_code)
path


# %%
transform(path)
