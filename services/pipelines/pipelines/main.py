from dags.exchanges.jobs.config import ExchangesPath
from shared.clients.api.iso.client import IsoExchangesApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.utils.path.builder import UrlBuilder


def main():
    # upload = datalake_client.upload_file(
    #     "adf".encode(), ExchangesPath(file_type="csv", zone="temp", asset_source="EOD")
    # )
    # print(upload.dict())

    print(UrlBuilder.build_url("base", "/asdfasdf/", "asdf"))

    IsoExchangesApiClient.get_exchanges()
    # print(PathBuilder.build_file_path(file_name="/asdf", file_type="csv", file_path=["/asdf", "ASdf"]))


if __name__ == "__main__":
    main()
