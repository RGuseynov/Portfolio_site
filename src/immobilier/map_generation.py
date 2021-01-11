import json

import pandas as pd 
import numpy as np
import folium
import vincent
import branca
import branca.colormap as cm


class FoliumMap:
    def __init__(self, longitude, latitude):
        self.longitude = longitude
        self.latitude = latitude
        self.map = folium.Map(
            location=[longitude, latitude],
            tiles='openstreetmap',
            zoom_start=8,
            attr='My Data Attribution'
        )

    def draw_departement(self, geodata, row=None):
        '''row: pandas.series'''
        choro = folium.Choropleth(
            geo_data=geodata,
            fill_color=row["color"] if row is not None else "white",
            fill_opacity=0.3,
            line_opacity=1,
            line_weight=1,
            line_color='blue',
        )

        if row is not None:
            # filter index names and build 3 columns from one(series)
            data = {
                    'decile_1': row.filter(regex=".*decile_1$").values,
                    'decile_9': row.filter(regex=".*decile_9$").values,
                    'median': row.filter(like="median").values,
                    }
            df = pd.DataFrame.from_dict(data)
            row = row.drop("color")
            df.index = pd.to_datetime(list(dict.fromkeys([int(annee_c[:4]) for annee_c in row.index.tolist()])), format="%Y")

            line_chart = vincent.Line(df,
                                    width=300,
                                    height=200)
            line_chart.axis_titles(x='Année', y='prix m2')
            line_chart.legend(title=geodata["features"][0]["properties"]["nom"])
            popup = folium.Popup()
            folium.Vega(line_chart, width = 400, height=250).add_to(popup)

            popup.add_to(choro)
        choro.add_to(self.map)
        colormap.add_to(self.map)
        # self.map.add_child(colormap)

    def save(self, filepath='templates/immobilier/test2.html'):
        # Save to html
        self.map.save(filepath)


if __name__ == "__main__":

    df = pd.read_csv("data/immobilier/data_clean/m2_appartement_price_per_departement.csv", index_col=0)

    # istance of a LinearColormap for departement coloration
    # LinearColormap class was modified from source, this code will not work with branca library from pip 
    # (added index_display property for rendering)
    colormap = cm.LinearColormap(colors=['darkgreen', 'green', 'yellow', 'orange', 'red', 'darkred'],
                             index=[700, 1300, 2000, 2800, 5000, 10000], 
                             index_display=[700, 1500, 2500, 4500, 7000, 10000],
                             vmin=700, vmax=10000,
                             caption="Prix median du m2 pour un appartement par département")

    # normal Colormap class utilisation, work with branca library from pip
    # colormap = cm.LinearColormap(colors=['darkgreen', 'green', 'yellow', 'orange', 'red', 'darkred'],
    #                          index=[700, 1300, 2000, 2800, 5000, 10000], 
    #                          vmin=700, vmax=10000,
    #                          caption="Prix median du m2 pour un appartement par département")


    df["color"] = df["2019_median"].apply(colormap)

    longitude, latitude = 48.8566, 2.3522
    map1 = FoliumMap(longitude, latitude)

    with open("data/immobilier/geo_data/departements.geojson.txt", "r") as file:
        json_departements = json.load(file)

    for departement in json_departements["features"]:
        geodata = {"type": "FeatureCollection", "features": [departement]}
        if departement["properties"]["code"] in df.index.tolist():
            map1.draw_departement(geodata, df.loc[departement["properties"]["code"],:])
        else:
            map1.draw_departement(geodata)

    map1.save("templates/immobilier/map_folium_appartement.html")
