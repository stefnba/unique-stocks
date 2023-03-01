import include.jobs.exchanges.market_stack as market_stack
from include.jobs.exchanges.eod import (
    download_details_for_exchanges,
    download_exchanges,
    transform_exchanges,
)
from include.jobs.exchanges.iso import donwload_iso_exchange_list


def main():
    donwload_iso_exchange_list()
    market_stack.download_exchanges()
    download_exchanges()
    download_details_for_exchanges()
    transform_exchanges(
        "raw/exchanges/EodHistoricalData/2023/02/25/\
            20230225-114616_EodHistoricalData.json"
    )


if __name__ == "__main__":
    main()
