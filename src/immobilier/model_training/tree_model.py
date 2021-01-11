import pickle

import pandas as pd
import numpy as np

from sklearn import datasets, tree
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn import metrics


pd.set_option('display.float_format', lambda x: '%.5f' % x)
# pd.set_option('display.max_columns', None)
pd.set_option('display.width', 120)
pd.set_option('display.max_rows', 100)
pd.set_option('display.min_rows', 30)


df = pd.read_csv("data/data_clean/appartements.csv", index_col=0, dtype={"code_departement":str, "code_commune":str})
departement_prices_df = pd.read_csv("data/data_clean/m2_appartement_price_per_departement.csv", index_col=0)

df = df.dropna()

# drop outre-mer for more metropolitan precision
df = df[~df["code_departement"].isin(["971", "972", "973", "974"])]


df.loc[df["code_departement"] == "2A", "code_departement"] = 100
df.loc[df["code_departement"] == "2B", "code_departement"] = 101
df.loc[df["code_departement"] == 100, "code_commune"] = df.loc[df["code_departement"] == 100, "code_commune"].apply(lambda x: int(x[-3:]) + 100000)
df.loc[df["code_departement"] == 101, "code_commune"] = df.loc[df["code_departement"] == 101, "code_commune"].apply(lambda x: int(x[-3:]) + 110000)
df["code_departement"] = df["code_departement"].astype(int)
df["code_commune"] = df["code_commune"].astype(int)

X = df[["surface_reelle_bati", "nombre_pieces_principales", "code_commune"]]
Y = df["valeur_fonciere"]

X_train, X_test, Y_train, Y_test = train_test_split(X, Y)

tr1 = tree.DecisionTreeRegressor()
tr1.fit(X_train, Y_train)
Y_predict = tr1.predict(X_test)

# print(scaler.inverse_transform(X_test))
print(X_test)
print(Y_test)
print(Y_predict)

print(metrics.r2_score(Y_test, Y_predict))
# print(metrics.explained_variance_score(Y_test, Y_predict))

with open("ml_models/tree1.pkl", 'wb') as file:
    pickle.dump(tr1, file)
