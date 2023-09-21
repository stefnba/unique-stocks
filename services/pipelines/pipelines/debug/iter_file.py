# %%
import sys
import os

sys.path.append("..")
os.chdir("..")

# %%


PATH = "/Users/stefanjakobbauer/Development/projects/unique-stocks/services/pipelines/pipelines/downloads/lei-isin-20230712T070126.csv"
# path = "/Users/stefanjakobbauer/Development/projects/unique-stocks/services/pipelines/pipelines/downloads/20230529-0800-gleif-goldencopy-lei2-golden-copy.csv"
from shared.utils.file.stream import StreamDiskFile


for a in StreamDiskFile(PATH).iter_content():
    print(a)
