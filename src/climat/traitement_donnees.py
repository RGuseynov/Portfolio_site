import pandas as pd 
import numpy as np
import json
import gzip
import tarfile
import glob
import tracemalloc


def process_a_year(folderPath:str, year:int) -> tuple:
    # tracemalloc.start()
    # current, peak = tracemalloc.get_traced_memory()
    # print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")

    df = pd.read_csv(f"{folderPath}/{year}.tar.gz", compression='gzip')
    df = df.rename({df.columns[0]: 'STATION'}, axis=1)
    df = df[df["DATE"] != "DATE"]
    df = df.dropna()

    # current, peak = tracemalloc.get_traced_memory()
    # print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
    # tracemalloc.stop()

    # type conversion
    float_columns = ["LATITUDE", "LONGITUDE", "ELEVATION", "TEMP", "MAX", "MIN", 
                    "DEWP", "VISIB", "WDSP", "MXSPD", "PRCP", "SNDP"]
    df[float_columns] = df[float_columns].astype(np.float32)
    df["STATION"] = df["STATION"].astype(np.int64)
    df["DATE"] = pd.to_datetime(df["DATE"])

    country_listPath = "data/climat/country_list.json"
    with open(country_listPath, 'rb') as file:
        country_dict = json.load(file)

    # df["COUNTRY"] = df["NAME"].str[-2:]
    df[["NAME", "COUNTRY"]] = df["NAME"].str.split(", ", n=2, expand=True)
    df = df[df["COUNTRY"].isin([key for key in country_dict.keys()])]
    df["COUNTRY"] = df["COUNTRY"].map(country_dict)
    df = df[(df["LATITUDE"] > 35) & (df["LATITUDE"] < 72)]
    df = df[~((df["COUNTRY"] == "Portugal") & (df["LONGITUDE"] < -15))]

    #STP, SLP, GUST, VISIB missing to much values
    df = df[["DATE", "STATION", "COUNTRY", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "TEMP", "MAX", "MIN",
            "DEWP", "WDSP", "MXSPD", "PRCP", "SNDP", "FRSHTT"]]
            
    df[["TEMP", "DEWP", "MAX", "MIN"]] = df[["TEMP", "DEWP", "MAX", "MIN"]].replace(9999.9, np.nan)
    df[["WDSP", "MXSPD"]] = df[["WDSP", "MXSPD"]].replace(999.9, np.nan)
    df["SNDP"] = df["SNDP"].replace(999.9, 0)
    df["PRCP"] = df["PRCP"].replace(99.9, 0)
    df[["FOG", "RAIN", "SNOW", "HAIL", "THUN", "TORN"]] = df["FRSHTT"].str.extract(r"(.)(.)(.)(.)(.)(.)")
    df[["FOG", "RAIN", "SNOW", "HAIL", "THUN", "TORN"]] = df[["FOG", "RAIN", "SNOW", "HAIL", "THUN", "TORN"]].astype(np.int8)
    df = df.drop("TORN", axis=1)

    # conversion Fahrenheit en Celsius, DWEP = point de rose
    df[["TEMP", "MAX", "MIN", "DEWP"]] = (df[["TEMP", "MAX", "MIN", "DEWP"]] -32) * 5/9
    # knots en kilometre par heure
    df[["WDSP", "MXSPD"]] = df[["WDSP", "MXSPD"]] * 1.852
    # inche en millimetre
    df["SNDP"] = df["SNDP"] * 2.54
    # inche and hundredths en millimetre
    df["PRCP"] = df["PRCP"] * 0.254

    # group by month: [df["DATE"].dt.to_period('m')] or pd.Grouper(key="DATE", freq="M")
    # aggregation
    df = df.groupby([df["DATE"].dt.to_period('m'), df["STATION"]]).agg(        
        NAME=("NAME","last"), COUNTRY=("COUNTRY","last"), 
        LATITUDE=("LATITUDE","last"), LONGITUDE=("LONGITUDE","last"), 
        ELEVATION=("ELEVATION","last"), DAYS_WITH_MEASURES=('TEMP','count'), TEMP=('TEMP','mean'), 
        MAX=("MAX",'max'), MIN=("MIN",'min'), DEWP=("DEWP",'mean'), WDSP=("WDSP",'mean'), 
        MXSPD=("MXSPD",'max'), SNDP=("SNDP",'sum'), PRCP=("PRCP",'sum'), FOG=("FOG",'sum'), 
        RAIN=("RAIN",'sum'), SNOW=("SNOW",'sum'), HAIL=("HAIL",'sum'), THUN=("THUN",'sum'))
    df = df.reset_index()
    
    df = df.groupby([df["DATE"], df["NAME"], df["COUNTRY"]]).agg(        
        STATION=("STATION","last"), LATITUDE=("LATITUDE","last"), LONGITUDE=("LONGITUDE","last"), 
        DAYS_WITH_MEASURES=("DAYS_WITH_MEASURES","sum"), ELEVATION=("ELEVATION","last"), TEMP=('TEMP','mean'), 
        MAX=("MAX",'max'), MIN=("MIN",'min'), DEWP=("DEWP",'mean'), WDSP=("WDSP",'mean'), 
        MXSPD=("MXSPD",'max'), SNDP=("SNDP",'mean'), PRCP=("PRCP",'mean'), FOG=("FOG",'mean'), 
        RAIN=("RAIN",'mean'), SNOW=("SNOW",'mean'), HAIL=("HAIL",'mean'), THUN=("THUN",'mean'))
    df = df.reset_index()
    
    df = df.groupby([df["DATE"], df["LATITUDE"], df["LONGITUDE"]]).agg(        
        STATION=("STATION","last"), NAME=("NAME","last"), COUNTRY=("COUNTRY","last"), 
        DAYS_WITH_MEASURES=("DAYS_WITH_MEASURES","sum"), ELEVATION=("ELEVATION","last"), TEMP=('TEMP','mean'), 
        MAX=("MAX",'max'), MIN=("MIN",'min'), DEWP=("DEWP",'mean'), WDSP=("WDSP",'mean'), 
        MXSPD=("MXSPD",'max'), SNDP=("SNDP",'mean'), PRCP=("PRCP",'mean'), FOG=("FOG",'mean'), 
        RAIN=("RAIN",'mean'), SNOW=("SNOW",'mean'), HAIL=("HAIL",'mean'), THUN=("THUN",'mean'))
    df = df.reset_index()

    df_drop = df[df["DAYS_WITH_MEASURES"] < 25]
    df = df[df["DAYS_WITH_MEASURES"] >= 25]
    df = df.drop("DAYS_WITH_MEASURES", axis=1)

    return df


def process_years(folderPath:str, begin_year:int, end_year:int, destinationPath:str):
    df_list = []
    for year in range(begin_year, end_year + 1):
        df = process_a_year(folderPath, year)
        df_list.append(df)
    dfs = pd.concat(df_list, ignore_index=True)

    # on maj les valeurs d'identification sur l'ensemble des annees pour correspondre au données les plus récentes
    dfs[["NAME", "COUNTRY", "LATITUDE", "LONGITUDE", "ELEVATION"]] = dfs[["NAME", "COUNTRY", "STATION", 
        "LATITUDE", "LONGITUDE", "ELEVATION"]].groupby(["STATION"]).transform('last')   
    # sert dans le cas où l'id STATION a changés dans le temps
    dfs[["STATION", "LATITUDE", "LONGITUDE", "ELEVATION"]] = dfs[["NAME", "COUNTRY", "STATION", 
        "LATITUDE", "LONGITUDE", "ELEVATION"]].groupby(["NAME", "COUNTRY"]).transform('last')
    # sert dans le cas ou une station(même emplacement en LAT et LONG) a changé d'id STATION et de NAME
    dfs[["NAME", "COUNTRY", "STATION", "ELEVATION"]] = dfs[["NAME", "COUNTRY", "STATION", 
        "LATITUDE", "LONGITUDE", "ELEVATION"]].groupby(["LATITUDE", "LONGITUDE"]).transform('last')

    # nombre de mois avec des mesures par station
    months_with_measures = dict(dfs["STATION"].value_counts())
    dfs["MONTHS_WITH_MEASURES"] = dfs["STATION"].map(months_with_measures)
    dfs_drop = dfs[dfs["MONTHS_WITH_MEASURES"] < 6]
    dfs = dfs[dfs["MONTHS_WITH_MEASURES"] >= 6]

    dfs_geo_dim = dfs[["STATION", "NAME", "COUNTRY", "LATITUDE", "LONGITUDE", "ELEVATION"]].drop_duplicates("STATION")
    dfs_fact = dfs.drop(["NAME", "COUNTRY", "LATITUDE", "LONGITUDE", "ELEVATION",
        "MONTHS_WITH_MEASURES"], axis=1)

    dfs_fact.to_csv(f"{destinationPath}/ClimatFACT.csv", index=False)
    dfs_geo_dim.to_csv(f"{destinationPath}/StationDIM.csv", index=False)


if __name__ == "__main__":
    pd.set_option('display.float_format', lambda x: '%.5f' % x)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.min_rows', 30)

    process_years("data/climat/daily_raw", 2000, 2020, "data/climat/clean_for_bi")
