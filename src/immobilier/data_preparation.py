import gzip
import os

import pandas as pd


pd.set_option('display.float_format', lambda x: '%.5f' % x)
# pd.set_option('display.max_columns', None)
pd.set_option('display.width', 120)
pd.set_option('display.max_rows', 100)
pd.set_option('display.min_rows', 30)


def appartement_preparation(df: pd.DataFrame) -> pd.DataFrame:
    # keep only columns for work on
    df = df[["nature_mutation", "id_mutation", "date_mutation", "valeur_fonciere", "type_local", 
    "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain", "code_commune", 
    "code_departement", "code_postal", "longitude", "latitude"]]
    # keep only sales
    df = df[df["nature_mutation"] == "Vente"]
    # deleting all sales with multiple lots to keep it simple and more accurate
    df = df.drop_duplicates(subset=["id_mutation"], keep=False)
    # select only appartments 
    df = df[df["type_local"] == "Appartement"]
    # keep only appartements without lands
    df = df[df["surface_terrain"].isna()]
    # calculate price per square metter and delete the most aberants entries
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    df = df.loc[(df["prix_m2"] > 500) & (df["prix_m2"] < 20000),:]
    # keep only appartements greater than minimum viable
    df = df[df["surface_reelle_bati"] >= 9]
    # convert code departement to string mainly because of 2A and 2B departement and for geojson mapping
    df["code_departement"] = df["code_departement"].astype(str)
    # put a "0" in front of first 9 departements for mapping data and joins for later
    df["code_departement"] = df["code_departement"].apply(lambda x: x if len(x)>1 else "0"+x)

    df = df[["date_mutation", "valeur_fonciere", "surface_reelle_bati", "nombre_pieces_principales", 
    "code_departement", "code_postal" ,"code_commune", "longitude", "latitude", "prix_m2"]]
    return df


def maison_preparation(df: pd.DataFrame) -> pd.DataFrame:
    # keep only columns for work on
    df = df[["nature_mutation", "id_mutation", "date_mutation", "valeur_fonciere", "type_local", 
    "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain", "code_commune", 
    "code_departement", "code_postal", "longitude", "latitude"]]
    # keep only sales
    df = df[df["nature_mutation"] == "Vente"]
    # deleting all sales with multiple lots to keep it simple and more accurate
    df = df.drop_duplicates(subset=["id_mutation"], keep=False)
    # select only appartments 
    df = df[df["type_local"] == "Maison"]

    # calculate price per square metter and delete the most aberants entries
    df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]
    df = df.loc[(df["prix_m2"] > 500) & (df["prix_m2"] < 20000),:]
    # keep only appartements greater than minimum viable
    df = df[df["surface_reelle_bati"] >= 9]
    # convert code departement to string mainly because of 2A and 2B departement and for geojson mapping
    df["code_departement"] = df["code_departement"].astype(str)
    # put a "0" in front of first 9 departements for mapping data and joins for later
    df["code_departement"] = df["code_departement"].apply(lambda x: x if len(x)>1 else "0"+x)

    df = df[["date_mutation", "valeur_fonciere", "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain",
    "code_departement", "code_postal" ,"code_commune", "longitude", "latitude", "prix_m2"]]
    return df


def data_work(preparation_function:callable) -> tuple:

    def custom_agg(x) -> pd.Series:
        d = {}
        d[f"{year}_median"] = x["prix_m2"].median()
        d[f"{year}_decile_1"] = x["prix_m2"].quantile(0.1)
        d[f"{year}_decile_9"] = x["prix_m2"].quantile(0.9)
        return pd.Series(d, index=[f"{year}_median", f"{year}_decile_1", f"{year}_decile_9"])

    # will contain clean dfs mostly for ml
    clean_df_list = []
    # will contain yearly departement aggregated prices, dfs mostly for map generation
    yearly_departement_prices_df_list = []

    for year in range(2014, 2021):
        with gzip.open(f'data/immobilier/transactions_raw/full{year}.csv.gz', 'rb') as f:
            df = pd.read_csv(f)
        # apply the desired preparation function
        prepared_df = preparation_function(df)
        # append the yearly prepared data to list of prepared dataframes
        clean_df_list.append(prepared_df)

        # aggregate data by departements by computing the median and deciles
        aggregated_departement_price = prepared_df[["code_departement", "prix_m2"]].groupby(["code_departement"]).apply(custom_agg) 
        # append the yearly data to list of dataframes
        yearly_departement_prices_df_list.append(aggregated_departement_price)

    clean_df = pd.concat(clean_df_list, axis=0)
    yearly_departement_prices_df = pd.concat(yearly_departement_prices_df_list, axis=1)

    clean_df = clean_df.set_index("date_mutation")
    clean_df.index = pd.to_datetime(clean_df.index)

    return clean_df, yearly_departement_prices_df


def save_essential_data():
    df_list = []
    for year in range(2014, 2021):
        with gzip.open(f'data/transactions_raw/full{year}.csv.gz', 'rb') as f:
            df = pd.read_csv(f)
        df_app = appartement_preparation(df)
        print(df_app["code_departement"].unique().tolist())
        df_list.append(df_app)
    dfs = pd.concat(df_list)
    dfs = dfs.set_index("date_mutation")
    dfs.index = pd.to_datetime(dfs.index)
    dfs.to_csv("data/immobilier/data_clean/appartements.csv")


if __name__ == "__main__":
    # for year in range(2019, 2020):
    #     with gzip.open(f'data/immobilier/transactions_raw/full{year}.csv.gz', 'rb') as f:
    #         df = pd.read_csv(f)
    # df = maison_preparation(df)

    result1, result2 = data_work(maison_preparation)
    result1.to_csv("data/immobilier/data_clean/maison.csv")
    result2.to_csv("data/immobilier/data_clean/m2_maison_price_per_departement.csv")
