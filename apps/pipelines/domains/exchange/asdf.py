from collections.abc import Callable
from datetime import date

from core.ingestion import BronzeParseResult
from core.models import BronzeModel, ProviderModel
from domains.exchange.models import ExchangeSnapshot
from providers.eodhd.models import SupportedExchange


# --- The Pure Functional Generic Factory ---
def create_parser[P: ProviderModel, D: BronzeModel](
    model: type[P], mapping: Callable[[P], D]
) -> Callable[[P], BronzeParseResult[D]]:
    """Generates a type-safe parsing function based on a model and a mapping closure."""

    def parse(raw: P) -> BronzeParseResult[D]:
        # todo validate the raw data against the model
        validated = model.model_validate(raw)
        row = mapping(validated)
        return BronzeParseResult(row=row, raw_fragment=raw)

    return parse


# --- Instantiating the parser inline with zero class overhead ---
parse_exchange = create_parser(
    model=SupportedExchange,
    mapping=lambda raw: ExchangeSnapshot(
        # snapshot_date=snapshot_date,
        country=raw.country,
        currency=raw.currency,
        country_iso2=raw.country_iso2,
        country_iso3=raw.country_iso3,
        operating_mic=raw.operating_mic,
        exchange_code=raw.exchange_code,
        name=raw.name,
        snapshot_date=date.today(),
    ),
)

# --- Execution ---
raw_data = SupportedExchange(
    Code="XNYS", Name="NYSE", Country="USA", Currency="USD", CountryISO2="US", CountryISO3="USA", OperatingMIC="XNYS"
)

# Your IDE perfectly infers that 'result' is a BronzeParseResult[ExchangeSnapshot]
result = parse_exchange(raw_data)
print(result)
