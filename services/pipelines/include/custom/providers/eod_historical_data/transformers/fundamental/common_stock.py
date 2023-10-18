import json
import polars as pl
from custom.providers.eod_historical_data.transformers.utils import deep_get
from custom.providers.eod_historical_data.transformers.fundamental.base import EoDFundamentalTransformer, Period


class Schema:
    Listing = [
        ("Code", pl.Utf8),
        ("Exchange", pl.Utf8),
    ]
    Valuation = [
        ("TrailingPE", pl.Utf8),
        ("ForwardPE", pl.Utf8),
        ("PriceSalesTTM", pl.Utf8),
        ("PriceBookMRQ", pl.Utf8),
        ("EnterpriseValue", pl.Utf8),
        ("EnterpriseValueRevenue", pl.Utf8),
        ("EnterpriseValueEbitda", pl.Utf8),
    ]
    ShareStat = [
        ("SharesOutstanding", pl.Utf8),
        ("SharesFloat", pl.Utf8),
        ("PercentInsiders", pl.Utf8),
        ("PercentInstitutions", pl.Utf8),
        ("SharesShort", pl.Utf8),
        ("SharesShortPriorMonth", pl.Utf8),
        ("ShortRatio", pl.Utf8),
        ("ShortPercentOutstanding", pl.Utf8),
        ("ShortPercentFloat", pl.Utf8),
    ]
    ShareOutstanding = [
        ("dateFormatted", pl.Utf8),
        ("shares", pl.Int64),
    ]
    ShareSplit = [
        ("LastSplitFactor", pl.Utf8),
        ("LastSplitDate", pl.Utf8),
    ]
    Dividend = [
        ("ForwardAnnualDividendRate", pl.Float64),
        ("ForwardAnnualDividendYield", pl.Float64),
        ("PayoutRatio", pl.Float64),
        ("DividendDate", pl.Utf8),
        ("ExDividendDate", pl.Utf8),
    ]
    Technical = [
        ("Beta", pl.Float64),
        ("52WeekHigh", pl.Float64),
        ("52WeekLow", pl.Float64),
        ("50DayMA", pl.Float64),
        ("200DayMA", pl.Float64),
        ("SharesShort", pl.Int64),
        ("SharesShortPriorMonth", pl.Int64),
        ("ShortRatio", pl.Float64),
        ("ShortPercent", pl.Float64),
    ]
    People = [
        ("Name", pl.Utf8),
        ("Title", pl.Utf8),
        ("YearBorn", pl.Utf8),
    ]
    Highlight = [
        ("MarketCapitalization", pl.Utf8),
        ("MarketCapitalizationMln", pl.Utf8),
        ("EBITDA", pl.Utf8),
        ("PERatio", pl.Utf8),
        ("PEGRatio", pl.Utf8),
        ("WallStreetTargetPrice", pl.Utf8),
        ("BookValue", pl.Utf8),
        ("DividendShare", pl.Utf8),
        ("DividendYield", pl.Utf8),
        ("EarningsShare", pl.Utf8),
        ("EPSEstimateCurrentYear", pl.Utf8),
        ("EPSEstimateNextYear", pl.Utf8),
        ("EPSEstimateNextQuarter", pl.Utf8),
        ("EPSEstimateCurrentQuarter", pl.Utf8),
        ("MostRecentQuarter", pl.Utf8),
        ("ProfitMargin", pl.Utf8),
        ("OperatingMarginTTM", pl.Utf8),
        ("ReturnOnAssetsTTM", pl.Utf8),
        ("ReturnOnEquityTTM", pl.Utf8),
        ("RevenueTTM", pl.Utf8),
        ("RevenuePerShareTTM", pl.Utf8),
        ("QuarterlyRevenueGrowthYOY", pl.Utf8),
        ("GrossProfitTTM", pl.Utf8),
        ("DilutedEpsTTM", pl.Utf8),
        ("QuarterlyEarningsGrowthYOY", pl.Utf8),
    ]
    General = [
        ("Code", pl.Utf8),
        ("Type", pl.Utf8),
        ("Name", pl.Utf8),
        ("Exchange", pl.Utf8),
        ("CurrencyCode", pl.Utf8),
        ("CountryISO", pl.Utf8),
        ("OpenFigi", pl.Utf8),
        ("ISIN", pl.Utf8),
        ("LEI", pl.Utf8),
        ("PrimaryTicker", pl.Utf8),
        ("CUSIP", pl.Utf8),
        ("CIK", pl.Utf8),
        ("EmployerIdNumber", pl.Utf8),
        ("FiscalYearEnd", pl.Utf8),
        ("IPODate", pl.Utf8),
        ("InternationalDomestic", pl.Utf8),
        ("Sector", pl.Utf8),
        ("Industry", pl.Utf8),
        ("GicSector", pl.Utf8),
        ("GicGroup", pl.Utf8),
        ("GicIndustry", pl.Utf8),
        ("GicSubIndustry", pl.Utf8),
        ("HomeCategory", pl.Utf8),
        ("IsDelisted", pl.Boolean),
        ("Description", pl.Utf8),
        (
            "AddressData",
            pl.Struct(
                [
                    pl.Field("Street", pl.Utf8),
                    pl.Field("City", pl.Utf8),
                    pl.Field("State", pl.Utf8),
                    pl.Field("Country", pl.Utf8),
                    pl.Field("ZIP", pl.Utf8),
                ]
            ),
        ),
        ("Phone", pl.Utf8),
        ("WebURL", pl.Utf8),
        ("LogoURL", pl.Utf8),
        ("FullTimeEmployees", pl.Utf8),
        ("UpdatedAt", pl.Utf8),
    ]


class EoDCommonStockFundamentalTransformer(EoDFundamentalTransformer):
    """
    Not implemented are:
    - AnalystRatings
    - Holders
    - InsiderTransactions
    - ESGScores
    - SplitsDividends.NumberDividendsByYear
    """

    type = "CommonStock"

    frames = [
        "general",
        "listing",
        "people",
        "balance_sheet_yearly",
        "balance_sheet_quarterly",
        "income_statement_quarterly",
        "income_statement_yearly",
        "cash_flow_statement_quarterly",
        "cash_flow_statement_yearly",
        "highlight",
        "valuation",
        "share_stat",
        "dividend",
        "technical",
        "share_split",
        "outstanding_shares_yearly",
        "outstanding_shares_quarterly",
        "earning_history",
        "earning_trend",
        "earning_per_share",
    ]

    def transform_listing(self):
        METRIC = "people"
        CATEGORY = "general"
        KEY = ["General", "Listings"]
        SCHEMA = Schema.Listing

        return self._transform_to_json(
            metric=METRIC,
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
        )

    def transform_people(self):
        METRIC = "people"
        CATEGORY = "general"
        KEY = ["General", "Officers"]
        SCHEMA = Schema.People

        return self._transform_to_json(
            metric=METRIC,
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
        )

    def transform_general(self):
        CATEGORY = "general"
        KEY = ["General"]
        SCHEMA = Schema.General

        return self._transform_from_dict(
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
            unnest_columns=["AddressData"],
        )

    def transform_balance_sheet_yearly(self):
        return self._transform_balance_sheet(period="yearly")

    def transform_balance_sheet_quarterly(self):
        return self._transform_balance_sheet(period="quarterly")

    def transform_income_statement_quarterly(self):
        return self._transform_income_statement(period="quarterly")

    def transform_income_statement_yearly(self):
        return self._transform_income_statement(period="yearly")

    def transform_cash_flow_statement_quarterly(self):
        return self._transform_cash_flow_statement(period="quarterly")

    def transform_cash_flow_statement_yearly(self):
        return self._transform_cash_flow_statement(period="yearly")

    def transform_outstanding_shares_quarterly(self):
        return self._transform_outstanding_shares(period="quarterly")

    def transform_outstanding_shares_yearly(self):
        return self._transform_outstanding_shares(period="yearly")

    def _transform_outstanding_shares(self, period: Period):
        CATEGORY = "outstanding_shares"
        KEYS = [
            "outstandingShares",
            "annual" if period == "yearly" else "quarterly",
        ]

        return self._transform_from_records(
            category=CATEGORY,
            period=period,
            data_keys=KEYS,
            date_column="dateFormatted",
            schema=Schema.ShareOutstanding,
        )

    def transform_earning_history(self):
        CATEGORY = "earning_history"
        KEYS = [
            "Earnings",
            "History",
        ]
        PERIOD = "quarterly"

        return self._transform_from_records(
            category=CATEGORY,
            period=PERIOD,
            data_keys=KEYS,
            published_column="reportDate",
        )

    def transform_earning_trend(self):
        CATEGORY = "earning_trend"
        KEYS = [
            "Earnings",
            "Trend",
        ]
        PERIOD = "quarterly"

        return self._transform_from_records(
            category=CATEGORY,
            period=PERIOD,
            data_keys=KEYS,
        )

    def transform_earning_per_share(self):
        CATEGORY = "earning_per_share"
        KEYS = [
            "Earnings",
            "Annual",
        ]
        PERIOD = "yearly"

        return self._transform_from_records(
            category=CATEGORY,
            period=PERIOD,
            data_keys=KEYS,
        )

    def transform_highlight(self):
        CATEGORY = "highlight"
        KEY = ["Highlights"]
        SCHEMA = Schema.Highlight

        return self._transform_from_dict(
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
        )

    def transform_valuation(self):
        CATEGORY = "valuation"
        KEY = ["Valuation"]
        SCHEMA = Schema.Valuation

        return self._transform_from_dict(
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
        )

    def transform_share_stat(self):
        CATEGORY = "share_stat"
        KEY = ["SharesStats"]
        SCHEMA = Schema.ShareStat

        return self._transform_from_dict(
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
        )

    def transform_technical(self):
        CATEGORY = "technical"
        KEY = ["Technicals"]
        SCHEMA = Schema.Technical

        return self._transform_from_dict(
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
        )

    def transform_dividend(self):
        CATEGORY = "dividend"
        KEY = ["SplitsDividends"]
        SCHEMA = Schema.Dividend

        return self._transform_from_dict(
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
        )

    def transform_share_split(self):
        CATEGORY = "share_split"
        KEY = ["SplitsDividends"]
        SCHEMA = Schema.ShareSplit

        return self._transform_from_dict(
            category=CATEGORY,
            data_key=KEY,
            data_schema=SCHEMA,
        )

    def _transform_income_statement(self, period: Period):
        CATEGORY = "income_statement"
        KEYS = ["Financials", "Income_Statement", period]

        return self._transform_from_records(category=CATEGORY, period=period, data_keys=KEYS)

    def _transform_cash_flow_statement(self, period: Period):
        CATEGORY = "cash_flow_statement"
        KEYS = ["Financials", "Cash_Flow", period]

        return self._transform_from_records(category=CATEGORY, period=period, data_keys=KEYS)

    def _transform_balance_sheet(self, period: Period):
        CATEGORY = "balance_sheet"
        KEYS = ["Financials", "Balance_Sheet", period]

        return self._transform_from_records(category=CATEGORY, period=period, data_keys=KEYS)
