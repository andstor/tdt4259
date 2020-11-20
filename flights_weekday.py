#%%
import pandas as pd
import glob
import datetime
import ntpath

path = r'bagtaggeneratedexport_mod'
all_files = glob.glob(path + "/dbo**.csv")

dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')

airportsFile = "./airports.csv"
dfAirports = pd.read_csv(
    airportsFile,
    index_col=None,
    header=0,
)

routes = pd.DataFrame()
for filename in all_files:
    dfBagtags = pd.read_csv(
        filename,
        index_col=None,
        header=0,
    )

    dfBagtags['sourceTimestamp'] = pd.to_datetime(dfBagtags['sourceTimestamp'], format='%Y-%m-%d %H:%M:%S.%f')
    dfBagtags['bagEventTimestamp'] = pd.to_datetime(dfBagtags['bagEventTimestamp'], format='%Y-%m-%d %H:%M:%S.%f')

    dfSum = dfBagtags.groupby([dfBagtags['sourceTimestamp'].dt.day_name(), 'LegArrayLength', 'Leg0_departureAirportIATA']).size().reset_index().rename(columns={"sourceTimestamp":"month", "Leg0_departureAirportIATA":"startLegIATA"})

    routes = pd.concat((routes,dfSum)).groupby(["month", 'startLegIATA'],as_index=False).sum()

routes['Flights'] = routes['LegArrayLength'] * routes[0]
routes.to_csv('./stats/flights_weekday.csv', index = False, header=True)
