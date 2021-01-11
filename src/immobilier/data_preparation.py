import gzip
import os

import pandas as pd


pd.set_option('display.float_format', lambda x: '%.5f' % x)
# pd.set_option('display.max_columns', None)
pd.set_option('display.width', 120)
pd.set_option('display.max_rows', 100)
pd.set_option('display.min_rows', 30)


def appartement_preparation(df: pd.DataFrame) -> pd.DataFrame:
    # keep only columns that will work on
    df = df[["nature_mutation", "id_mutation", "date_mutation", "valeur_fonciere", "type_local", 
    "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain", "code_commune", 
    "code_departement", "longitude", "latitude"]]
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


def save_yearly_price_per_departement():
    def custom_agg(x):
        d = {}
        d[f"{year}_median"] = x["prix_m2"].median()
        d[f"{year}_decile_1"] = x["prix_m2"].quantile(0.1)
        d[f"{year}_decile_9"] = x["prix_m2"].quantile(0.9)
        return pd.Series(d, index=[f"{year}_median", f"{year}_decile_1", f"{year}_decile_9"])

    df_list = []

    for year in range(2014, 2021):
        with gzip.open(f'data/immoblier/transactions_raw/full{year}.csv.gz', 'rb') as f:
            df = pd.read_csv(f)

            df_app = appartement_preparation(df)

            departement_app_price = df_app[["code_departement", "prix_m2"]].groupby(["code_departement"]).apply(custom_agg) 

            # append the yearly data to list of dataframes
            df_list.append(departement_app_price)

            print(departement_app_price)

    dfs = pd.concat(df_list, axis=1)

    # as_list = dfs.index.tolist()
    # for i in range(1, 10):
    #     idx = as_list.index(str(i))
    #     as_list[idx] = '0' + str(i)
    # dfs.index = as_list
    # dfs = dfs.sort_index()

    print(dfs)

    dfs.to_csv("data/immobilier/data_clean/m2_appartement_price_per_departement.csv")


def save_appartement_essential():
    df_list = []

    for year in range(2014, 2021):
        with gzip.open(f'data/transactions_raw/full{year}.csv.gz', 'rb') as f:
            df = pd.read_csv(f)

            df_app = appartement_preparation(df)

            print(df_app["code_departement"].unique().tolist())

            df_list.append(df_app)
            print(df_app)

    dfs = pd.concat(df_list)

    print(dfs)

    dfs = dfs.set_index("date_mutation")
    dfs.index = pd.to_datetime(dfs.index)

    print(dfs)

    dfs.to_csv("data/immobilier/data_clean/appartements.csv")
    median_df = pd.read_csv("data/immobilier/data_clean/m2_appartement_price_per_departement.csv")


if __name__ == "__main__":
    # with gzip.open(f'data/transactions_raw/full{2020}.csv.gz', 'rb') as f:
    #     df = pd.read_csv(f)
    #     print(df)

    save_appartement_essential()

    # save_yearly_price_per_departement()

    
