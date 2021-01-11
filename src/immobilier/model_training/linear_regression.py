import pandas as pd
import numpy as np

from sklearn import datasets, linear_model,
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.preprocessing import StandardScaler, Normalizer
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


df = df.merge(departement_prices_df["2019_median"], how="inner", left_on="code_departement", right_on="code_departement", validate="many_to_one") 

X = df[["surface_reelle_bati", "nombre_pieces_principales", "2019_median"]]
Y = df["valeur_fonciere"]


scaler = StandardScaler()
scaler.fit(X)
X = scaler.transform(X)

# normaliser1 = Normalizer()
# normaliser1.fit(X)
# X = normaliser1.transform(X)

X_train, X_test, Y_train, Y_test = train_test_split(X, Y)

lr1 = linear_model.LinearRegression()
lr1.fit(X_train, Y_train)
Y_predict = lr1.predict(X_test)

print(lr1.coef_)
print(lr1.intercept_)
print(mean_squared_error(Y_test, Y_predict))

print(metrics.r2_score(Y_test, Y_predict))
# print(metrics.explained_variance_score(Y_test, Y_predict))

