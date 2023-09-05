# %%
import pprint
from shared.data_lake_path import FundamentalPath, TempDirectory, TempFile


pprint.pprint(FundamentalPath.raw(source="EodHistoricalData", format="json").add_element(entity="AAPL").serialized)
# pprint.pprint(TempDirectory().serialized)
# pprint.pprint(TempDirectory(base_dir="telkasdlkf").serialized)
# pprint.pprint(TempFile().serialized)
