# %%

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

service_client = BlobServiceClient(
    account_url="https://uniquestocks.dfs.core.windows.net/", credential=DefaultAzureCredential()
)

path = "zone%3Draw/product%3Dindex/source%3DEodHistoricalData/year%3D2023/month%3D04/day%3D14/ts%3D20230414_163418__product%3Dindex__source%3DEodHistoricalData__zone%3Draw.json"

blob_client = service_client.get_blob_client(container="", blob="data-lake/test.txt")
a = blob_client.download_blob().readall()
a
# blob_client.upload_blob(a, blob_type="")
# blob_client.create_append_blob(data=b"NASDNNASD")
