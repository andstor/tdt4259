# Number of BagTags grouped by month.

import pandas as pd
import glob
import datetime
import ntpath

path = r'bagtaggeneratedexport_mod'
all_files = glob.glob(path + "/dbo**.csv")

dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')

routes = pd.DataFrame()
for filename in all_files:
    dfBagtags = pd.read_csv(
        filename,
        index_col=None,
        header=0,
    )
    dfBagtags['sourceTimestamp'] = pd.to_datetime(dfBagtags['sourceTimestamp'], format='%Y-%m-%d %H:%M:%S.%f')
    dfBagtags['bagEventTimestamp'] = pd.to_datetime(dfBagtags['bagEventTimestamp'], format='%Y-%m-%d %H:%M:%S.%f')

    dfSum = dfBagtags.groupby([dfBagtags['sourceTimestamp'].dt.to_period("M"), 'Leg0_departureAirportIATA']).size().reset_index().rename(columns={"sourceTimestamp":"month", "Leg0_departureAirportIATA":"startLegIATA"})

    routes = pd.concat((routes,dfSum)).groupby(["month", 'startLegIATA'],as_index=False).sum()

routes.to_csv('./stats/bagTags_corona_month.csv', index = False, header=True)
