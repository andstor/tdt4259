#%%
import pandas as pd
import glob
import datetime
import ntpath

path = r'bagtaggeneratedexport_mod'
all_files = glob.glob(path + "/dbo**.csv")

routes = pd.DataFrame()
for filename in all_files:
    dfBagtags = pd.read_csv(
        filename,
        index_col=None,
        header=0,
    )
    df = pd.DataFrame()
    for i in range(8)[::2]:
        dfSum = dfBagtags.groupby(["Leg" + str(i) + "_departureAirportIATA", "Leg" + str(i + 1) + "_departureAirportIATA"]).size().reset_index().rename(columns={"Leg" + str(i) + "_departureAirportIATA":"departures", "Leg" + str(i+1) + "_departureAirportIATA":"arrivals"})
        df = pd.concat((df,dfSum)).groupby(["departures", "arrivals"],as_index=False).sum()
    routes = pd.concat((routes,df)).groupby(["departures", "arrivals"],as_index=False).sum()

routes.to_csv('./stats/routes.csv', index = False, header=True)
