from airflow.datasets import Dataset

from shared.path import SecurityPath, SecurityQuotePath

SecurityQuote = Dataset(SecurityQuotePath.curated().uri)
SecurityPath = Dataset(SecurityPath.curated().uri)
