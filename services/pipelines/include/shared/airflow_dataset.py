from airflow.datasets import Dataset
from shared.path import SecurityQuotePath, SecurityPath

SecurityQuote = Dataset(SecurityQuotePath.curated().uri)
SecurityPath = Dataset(SecurityPath.curated().uri)
