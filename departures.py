import pandas as pd
import glob
import datetime
import ntpath

path = r'bagtaggeneratedexport_mod'
all_files = glob.glob(path + "/dbo**.csv")

departures = pd.DataFrame()
for filename in all_files:
    dfBagtags = pd.read_csv(
        filename,
        index_col=None,
        header=0,
    )
    df = pd.DataFrame()
    for i in range(8):
        dfSum = dfBagtags.groupby("Leg" + str(i) + "_departureAirportIATA").size().reset_index().rename(columns={"Leg" + str(i) + "_departureAirportIATA":"IATACode"})
        df = pd.concat((df,dfSum)).groupby('IATACode',as_index=False).sum()
    departures = pd.concat((departures,df)).groupby('IATACode',as_index=False).sum()

departures.to_csv('./stats/departures.csv', index = False, header=True)
