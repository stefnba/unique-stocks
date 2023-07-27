# %%
import sys
import os

sys.path.append("..")
os.chdir("..")

# %%


from shared.clients.api.gleif.client import GleifApiClient


GleifApiClient.get_entity_isin_mapping()


# from shared.hooks.api.api_hook import ApiHook


# hook = ApiHook()


# hook._base_url = ""

# hook._download_file_to_disk(
#     endpoint="https://mapping.gleif.org/api/v2/isin-lei/d6996d23-cdaf-413e-b594-5219d40f3da5/download",
#     file_destination="afsfsdf.zip",
# )


# %%

from shared.utils.path.container.file_path import container_file_path

container_file_path("zip")
