from airflow.datasets import Dataset

from shared.path import EntityPath, ExchangePath, SecurityPath, SecurityQuotePath

SecurityQuote = Dataset(SecurityQuotePath.curated().uri)
Security = Dataset(SecurityPath.curated().uri)
Exchange = Dataset(ExchangePath.curated().uri)
Entity = Dataset(EntityPath.curated().uri)
