import pandas as pd 
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from functools import reduce


class ClustersHandler:
    def __init__(self, df: pd.DataFrame, df_stations: pd.DataFrame, country:str=None, weights:dict=None):
        if country is not None:
            df_stations = df_stations[df_stations["COUNTRY"] == country]
        self.df = pd.merge(left=df, right=df_stations, how="right", left_on="STATION", right_on="STATION")

        self._normalise()
        if weights is not None:
            self._apply_weights(weights)

        normalised_df_stations = self.df[["STATION", "COUNTRY", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION"]].drop_duplicates("STATION")
        # effacement pour l'aggregation des stats par mois sinon latitude longitude etc seront mensualises
        self.df = self.df.drop(["COUNTRY", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION"], axis=1)

        self.df["DATE"] = pd.to_datetime(self.df["DATE"])
        self.df = self.df.groupby([self.df["STATION"], self.df["DATE"].dt.month.rename("MONTH")]).mean()
        self.df = self.df.unstack(level=1)
        self.df = self.df.dropna()
        # refusion avec les donnes stations dont lat long elevation normalise
        self.df = pd.merge(left=self.df, right=normalised_df_stations, how="inner", left_on="STATION", right_on="STATION")

        self._k_scores = None

    def _normalise(self):
        column_to_not_normalise = ["DATE", "STATION", "COUNTRY", "NAME"]
        column_to_normalise = [column for column in self.df.columns if column not in column_to_not_normalise]
        self.df[column_to_normalise] = ((self.df[column_to_normalise] - self.df[column_to_normalise].min())
                            / (self.df[column_to_normalise].max() - self.df[column_to_normalise].min()))

    def _apply_weights(self, weights:dict):
        for col_name, weight in weights.items():
            self.df[col_name] = self.df[col_name] * weight

    def get_clusters(self, clusters_name: str, list_columns: list, k_clusters_min: int=2, k_clusters_max: int=10) -> pd.DataFrame:
        '''list_columns: nom des colonnes de niveau 1 dans la hiérarchie, le niveau 2 étant le numéro du mois
        retourne un dataframe contenant les résultats des clusterisations'''
        # init du df résultats avec les id des stations qui seront clusterises
        df_results = self.df[["STATION"]].copy()
        # contiendra la liste de tuple des colonnes (niveau1, niveau2)
        list_columns_final = []
        for column in list_columns:
            if column in self.df.columns:
                list_columns_final.append(column)
            else:
                for n_month in range(1, 13):
                    list_columns_final.append((column, n_month))
        # initialisation du dictionnaire qui contiendra les scores des clusterisations pour chaque K
        k_range = range(k_clusters_min, k_clusters_max + 1)
        k_scores = {"K": list(k_range), f"{clusters_name}_inertia": [], f"{clusters_name}_silhouette": []}
        # plusieurs clusterisations en variant le K et ajout des résultats dans le df_results
        for k in k_range:
            K_means = KMeans(k, random_state=0).fit(self.df[list_columns_final])  
            print(self.df[list_columns_final])     
            name_colonne = k
            df_results.loc[:,name_colonne] = K_means.labels_ + 1

            # Sum of squared distances of samples to their closest cluster center
            k_scores[f"{clusters_name}_inertia"].append(K_means.inertia_)
            # Silhouete_score
            k_scores[f"{clusters_name}_silhouette"].append(silhouette_score(self.df[list_columns_final], K_means.labels_))
        # sauvegarde des scores en tant que membre de la classe et retour des clusterisations en output
        # si des scores sont deja présent (créer durant la même session de clusterisation à partir de la même 
        # instance de la classe ClusterHandler), on les réunit
        if self._k_scores is not None:
            last_scores = pd.DataFrame.from_dict(k_scores, orient="columns")
            self._k_scores = pd.merge(left=self._k_scores, right=last_scores, on="K", how="outer")
        else:
            self._k_scores = pd.DataFrame.from_dict(k_scores, orient="columns")      
        return df_results

    def get_k_scores(self) -> pd.DataFrame:
        return self._k_scores


def create_clusters():
    df = pd.read_csv("data/climat/clean_for_bi/ClimatFACT.csv")
    df_stations = pd.read_csv("data/climat/clean_for_bi/StationDIM.csv")

    clustering = ClustersHandler(df, df_stations, "France")

    clusters1 = clustering.get_clusters("Cluster_TEMP", ["TEMP"], 2, 10)
    clusters1 = clusters1.melt(id_vars=["STATION"], var_name="K", value_name="Cluster_TEMP")

    clusters2 = clustering.get_clusters("Cluster_TEMP+", ["TEMP", "MIN", "MAX", "DEWP"])
    clusters2 = clusters2.melt(id_vars=["STATION"], var_name="K", value_name="Cluster_TEMP+")

    clusters3 = clustering.get_clusters("Cluster_WIND", ["WDSP", "MXSPD"])
    clusters3 = clusters3.melt(id_vars=["STATION"], var_name="K", value_name="Cluster_WIND")

    clusters4 = clustering.get_clusters("Cluster_FRSHT", ["FOG", "RAIN", "SNOW", "HAIL", "THUN"])
    clusters4 = clusters4.melt(id_vars=["STATION"], var_name="K", value_name="Cluster_FRSHT")

    clusters5 = clustering.get_clusters("Cluster_ALL", ["TEMP", "MIN", "MAX", "DEWP", "WDSP", "MXSPD", 
                                        "FOG", "RAIN", "SNOW", "HAIL", "THUN"])
    clusters5 = clusters5.melt(id_vars=["STATION"], var_name="K", value_name="Cluster_ALL")

    clusters6 = clustering.get_clusters("Cluster_ALL+GEO", ["TEMP", "MIN", "MAX", "DEWP", "WDSP", "MXSPD", 
                "FOG", "RAIN", "SNOW", "HAIL", "THUN", "ELEVATION", "LATITUDE", "LONGITUDE"])
    clusters6 = clusters6.melt(id_vars=["STATION"], var_name="K", value_name="Cluster_ALL+GEO")

    all_clusters = [clusters1, clusters2, clusters3, clusters4, clusters5, clusters6]
    final_df = reduce(lambda left, right: pd.merge(left=left, right=right, on=["STATION", "K"], 
                                                    how="inner"), all_clusters)
    final_df.to_csv("data/climat/clean_for_bi/Clusters.csv", index=False)
    k_scores = clustering.get_k_scores()
    k_scores.to_csv("data/climat/clean_for_bi/k_scores.csv", index=False)


def create_optimal_cluster():
    df = pd.read_csv("data/climat/clean_for_bi/ClimatFACT.csv")
    df_stations = pd.read_csv("data/climat/clean_for_bi/StationDIM.csv")

    weights = {"TEMP":3, "MIN":2.5, "MAX":2.5, "DEWP":2, "WDSP":1.5, "MXSPD":1.5, 
                "FOG":1, "RAIN":2, "SNOW":1, "HAIL":1, "THUN":1.5, "ELEVATION":1, "LATITUDE":1, "LONGITUDE":1}
    clustering = ClustersHandler(df, df_stations, "France", weights)
    clusters = clustering.get_clusters("Cluster_ALL", [key for key in weights.keys()], 2, 10)
    clusters = clusters.melt(id_vars=["STATION"], var_name="K", value_name="Cluster_ALL")

    clusters.to_csv("data/climat/clean_for_bi/Clusters_opt.csv", index=False)


if __name__ == "__main__":
    pd.set_option('display.float_format', lambda x: '%.5f' % x)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.min_rows', 30)
    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)

    create_optimal_cluster()
    # create_clusters()
