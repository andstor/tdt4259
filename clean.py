import pandas as pd
import glob
import datetime
import ntpath

path = r'bagtaggeneratedexport' # use your path
all_files = glob.glob(path + "/dbo**.csv")

dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
#%%
for filename in all_files:

    df = pd.read_csv(
        filename,
        index_col=None,
        header=0,
        low_memory=False,
        parse_dates=True
        )

    df['sourceTimestamp'] = pd.to_datetime(df['sourceTimestamp'], format='%Y-%m-%d %H:%M:%S.%f')
    df['bagEventTimestamp'] = pd.to_datetime(df['bagEventTimestamp'], format='%Y-%m-%d %H:%M:%S.%f')

    tags = {}

    fromDate = df.iloc[1]['sourceTimestamp']
    keepTagDuration = datetime.timedelta(days=7)

    for index, row in df.iterrows():
        if row['bagTagNumber'] in tags:
            tags[row['bagTagNumber']].append(index)
        else:
            tags[row['bagTagNumber']] = [index]

    tags_res = [ x for x in tags.values() if len(x) > 1]

    df2 = df.loc[df.index.drop(sum(tags_res, []))]

    df2.to_csv('./bagtaggeneratedexport_mod/' + ntpath.basename(filename), index = False, header=True)
    print("Finished cleaning file " + ntpath.basename(filename))