import tarfile
import gzip
import numpy as np
import pandas as pd 
import os 
import dask.dataframe as dd 
from sklearn.cluster import KMeans


pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 40)


class Extractor:
    def __init__(self, source_path):
        self.source_path = source_path

    def tar_extract(self, year, destination_folder="extracted"):
        year_file = "gsod_" + str(year) + ".tar"
        with tarfile.open(self.source_path + year_file, "r") as file:
            file.extractall(destination_folder + "/" + str(year))

    def tar_extract_all(self, begin, end, destination_folder="extracted"):
        for i in range(begin, end+1):
            self.tar_extract(i, destination_folder)


class From_gz_to_csv:
    def __init__(self, source_path):
        self.source_path = source_path

    def append_gz_to_csv(self, gz, csv):
        columns_names = ["STN","WBAN","YEARMODA","TEMP","TEMP_C","DEWP","DEWP_C",
            "SLP","SLP_C","STP","STP_C","VISIB","VISIB_C","WDSP","WDSP_C","MXSPD","GUST","MAX","MIN","PRCP","SNDP","FRSHTT"]
        with gzip.open(gz, "r") as file:
            df = pd.read_csv(file, sep=r"\s+", skiprows=1, names=columns_names)
        if not os.path.isfile(csv):
            df.to_csv(csv, header=columns_names)
        else:
            df.to_csv(csv, mode='a', header=False)

    def convert(self, year, destination_folder="Meteo_CSV"):
        files = os.listdir(self.source_path + str(year))
        for file in files:
            self.append_gz_to_csv(self.source_path + str(year) + "/" + file, destination_folder + "/" + str(year) + ".csv")

    def convert_all(self, begin, end, destination_folder="Meteo_CSV"):
        for i in range(begin, end + 1):
            self.convert(i, destination_folder)


# execute once
# Ext = Extractor("noaa-global-surface-summary-of-the-day/gsod_all_years/")
# Ext.tar_extract_all(1901, 2019)

# csv_creator = From_gz_to_csv("extracted/")
# csv_creator.convert_all(1901, 2019)


class DataFrame_cleaner:
    def __init__(self, df):
        self.df = df

    def inner_join(self, df2):
        self.df = dd.merge(left=self.df, right=df2, how="inner", left_on=["STN", "WBAN"], right_on=["USAF", "WBAN"])

    def clean(self):
        self.df = self.df[["STN","WBAN","YEARMODA","TEMP","DEWP","SLP","STP","VISIB","WDSP",
        "MXSPD","GUST","MAX","MIN","PRCP","SNDP","FRSHTT"]]
        self.df["STN"] = self.df["STN"].apply(lambda x: x.zfill(6))
        self.df[["MAX", "MIN"]] = self.df[["MAX", "MIN"]].replace(r"\*", "", regex=True)
        self.df[["TEMP", "DEWP", "SLP", "STP", "MAX", "MIN"]] = self.df[["TEMP", "DEWP", "SLP", "STP", "MAX", "MIN"]].replace(9999.9, np.nan)
        self.df[["VISIB", "WDSP", "MXSPD", "GUST", "SNDP"]] = self.df[["VISIB", "WDSP", "MXSPD", "GUST", "SNDP"]].replace(999.9, np.nan)
        self.temp = self.df["PRCP"].str.extract(r"(\d*\.\d*)(.)")
        self.df["PRCP"] = self.temp[0].astype(float)
        self.df["PRCP"] = self.df["PRCP"].replace(99.99, np.nan)
        self.df["FRSHTT"] = self.df["FRSHTT"].apply(lambda x: str(x).zfill(6))
        self.temp = self.df["FRSHTT"].str.extract(r"(.)(.)(.)(.)(.)(.)")
        self.df[["FOG", "RAIN", "SNOW", "HAIL", "THUN", "TORN"]] = self.temp
        self.df = self.df.drop("FRSHTT", axis=1)

# ddf_france = dd.read_csv("isd-history.csv", dtype={"STATE": 'object', "USAF": 'object'})
# ddf_france = ddf_france[ddf_france["CTRY"] == "FR"]
# ddf_france = ddf_france[["USAF", "WBAN", "STATION NAME", "LAT", "LON", "ELEV(M)"]]

# ddf_meteo = dd.read_csv("Meteo_CSV/2018.csv", dtype={"STN": 'object'})
# cleaner = DataFrame_cleaner(ddf_meteo)
# cleaner.clean()
# cleaner.inner_join(ddf_france)

# cleaner.df.compute().to_csv("For_clustering_2018.csv")

class Clustering_Preparation:
    def __init__(self, df):
        self.df = df[["STATION NAME", "YEARMODA", "LAT", "LON", "ELEV(M)", "TEMP", "MAX", "MIN", "DEWP",
        "WDSP", "MXSPD", "PRCP", "FOG", "RAIN", "SNOW", "HAIL", "THUN", "TORN"]]

    @staticmethod
    def saison(row, column):
        if row["YEARMODA"] >= pd.to_datetime("20-03-2018") and row["YEARMODA"] <= pd.to_datetime("19-06-2018"):
            return pd.Series([row[column], np.nan, np.nan, np.nan])
        elif row["YEARMODA"] >= pd.to_datetime("20-06-2018") and row["YEARMODA"] <= pd.to_datetime("22-09-2018"):
            return pd.Series([np.nan, row[column], np.nan, np.nan])
        elif row["YEARMODA"] >= pd.to_datetime("23-09-2018") and row["YEARMODA"] <= pd.to_datetime("21-12-2018"):
            return pd.Series([np.nan, np.nan, row[column], np.nan])
        else:
            return pd.Series([np.nan, np.nan, np.nan, row[column]])

    def seasonalisation2(self, list_columns):
        self.df["YEARMODA"] = pd.to_datetime(self.df["YEARMODA"], format='%Y%m%d')
        for i in list_columns:
            spring = str(i) + "_SPRING"
            summer = str(i) + "_SUMMER"
            autumn = str(i) + "_AUTUMN"
            winter = str(i) + "_WINTER"
            self.df[[spring, summer, autumn, winter]] = self.df[["YEARMODA", i]].apply(lambda row: Clustering_Preparation.saison(row, i), axis=1)

    def seasonalisation(self, list_columns):
        self.df["YEARMODA"] = pd.to_datetime(self.df["YEARMODA"], format='%Y%m%d')
        for i in list_columns:
            spring = str(i) + "_SPRING"
            summer = str(i) + "_SUMMER"
            autumn = str(i) + "_AUTUMN"
            winter = str(i) + "_WINTER"
            self.df[spring] = self.df[["YEARMODA", i]].apply(lambda row: row[i] if row["YEARMODA"] > 
            pd.to_datetime("19-03-2018") and row["YEARMODA"] < pd.to_datetime("20-06-2018") else np.nan, axis=1)
            self.df[summer] = self.df[["YEARMODA", i]].apply(lambda row: row[i] if row["YEARMODA"] > 
            pd.to_datetime("19-06-2018") and row["YEARMODA"] < pd.to_datetime("23-09-2018") else np.nan, axis=1)
            self.df[autumn] = self.df[["YEARMODA", i]].apply(lambda row: row[i] if row["YEARMODA"] > 
            pd.to_datetime("22-09-2018") and row["YEARMODA"] < pd.to_datetime("22-12-2018") else np.nan, axis=1)
            self.df[winter] = self.df[["YEARMODA", i]].apply(lambda row: row[i] if (row["YEARMODA"] > 
            pd.to_datetime("21-12-2018") and row["YEARMODA"] < pd.to_datetime("31-12-2018")) or 
            (row["YEARMODA"] >= pd.to_datetime("01-01-2018") and row["YEARMODA"] <= pd.to_datetime("19-03-2018"))
            else np.nan, axis=1)
            
    def grouping(self):
        #self.df = self.df[["STATION NAME", "LAT", "LON", "ELEV(M)", "TEMP", "MAX", "MIN", "DEWP",
        #"WDSP", "MXSPD", "PRCP", "FOG", "RAIN", "SNOW", "HAIL", "THUN", "TORN"]]
        self.df = self.df.groupby("STATION NAME").mean()

    def normalisation(self):
        self.raw_df = self.df
        self.df = (self.df - self.df.min()) / (self.df.max() - self.df.min())

    def clustering(self, list_columns, k_clusters_min, k_clusters_max):
        self.df = self.df[list_columns]
        self.df[["LAT", "LON"]] = self.raw_df[["LAT", "LON"]]
        for i in range(k_clusters_min, k_clusters_max + 1):
            self.K_means = KMeans(i, random_state=0).fit(self.df[list_columns])
            name_colonne = "CLUST_" + str(i)
            self.df[name_colonne] = self.K_means.labels_


df = pd.read_csv("For_clustering_2018.csv")

clustering1 = Clustering_Preparation(df)
clustering1.seasonalisation2(["TEMP","PRCP"])
# print(clustering1.df["PRCP_SPRING"].describe())
# print(clustering1.df["PRCP_SUMMER"].describe())
# print(clustering1.df["PRCP_AUTUMN"].describe())
# print(clustering1.df["PRCP_WINTER"].describe())


clustering1.grouping()
clustering1.normalisation()
clustering1.df = clustering1.df.dropna(axis=0, how='any')
print(clustering1.df)


list_to_cluster = ["TEMP_SPRING","TEMP_SUMMER","TEMP_AUTUMN","TEMP_WINTER",
"PRCP_SPRING","PRCP_SUMMER","PRCP_AUTUMN","PRCP_WINTER"]
clustering1.clustering(list_to_cluster, 2, 10)


clustering1.df.to_csv("Clusters/Clusters_2018_TEMP_PRCP_SAISON.csv")


# print(clustering1.df[pd.isnull(clustering1.df).any(axis=1)])
