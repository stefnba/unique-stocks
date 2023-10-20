from airflow.datasets import Dataset

from shared.path import ExchangePath, SecurityPath, SecurityQuotePath

SecurityQuote = Dataset(SecurityQuotePath.curated().uri)
Security = Dataset(SecurityPath.curated().uri)
Exchange = Dataset(ExchangePath.curated().uri)
