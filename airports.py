#%%
import pandas as pd
import glob
import datetime
import ntpath

airportsFile = "./DimFlyplassMod.csv"

dfAirports = pd.read_csv(
    airportsFile,
    index_col=None,
    header=0,
)
dfAirports = dfAirports.drop_duplicates(subset="IATACode", keep="last")

dfAirports.to_csv('airports.csv', index = False, header=True)
