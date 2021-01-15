import json

import pandas as pd 
import numpy as np
import folium
from folium.plugins import FloatImage
import vincent
import branca
import branca.colormap as cm

from PIL import Image, ImageDraw, ImageFont


def create_title_image(title, image_path):
    W, H = (500,200)
    image = Image.new("RGBA",(W,H))
    draw = ImageDraw.Draw(image)
    w, h = draw.textsize(title)
    font = ImageFont.truetype('arial.ttf', 20)
    draw.text((0,0), title, font=font ,fill=(0, 0, 0))
    image.crop((0, 0,2*w,2*h)).save(image_path, "PNG")


class DepartementMap:
    def __init__(self, longitude, latitude, title):
        self.title = title
        self.map = folium.Map(
            location=[longitude, latitude],
            tiles='openstreetmap',
            zoom_start=6,
            attr='My Data Attribution'
        )
        self.fgroup_appart = folium.map.FeatureGroup(name="Appartement", overlay=True, control=True, show=True)
        self.fgroup_maison = folium.map.FeatureGroup(name="Maison", overlay=True, control=True, show=False)

    @staticmethod
    def make_line_chart_popup(data_row:pd.Series, title:str) -> folium.Popup:
        '''Create a line chart popup from temporal Series for departements
        Index of the Series have to be in {year}_median, {year}_decile1, {year}_decile9, {year+1}_median, {year+1}_decile1... format
        this popup can be added in map layers'''
        # filter index names and build 3 columns from one(series)
        data = {
                'decile_1': data_row.filter(regex=".*decile_1$").values,
                'decile_9': data_row.filter(regex=".*decile_9$").values,
                'median': data_row.filter(like="median").values,
                }
        df_to_display = pd.DataFrame.from_dict(data)
        data_row = data_row.drop("color")

        # create index of the dataframe from the inital data_row Series.index
        df_to_display.index = pd.to_datetime(list(dict.fromkeys([int(annee_c[:4]) for annee_c in data_row.index.tolist()])), format="%Y")

        line_chart = vincent.Line(df_to_display,
                                width=300,
                                height=200)
        line_chart.axis_titles(x='Année', y='prix m2')
        line_chart.legend(title=title)

        popup = folium.Popup()
        folium.Vega(line_chart, width = 400, height=250).add_to(popup)
        return popup

    def draw_departement(self, d_geodata, row_appart:pd.Series=None, row_maison:pd.Series=None) -> None:
        '''
        d_geodata: geodata for a departement
        row_appart: appartement m2 prices for this departement, None if departement data is missing
        row_maison: maison m2 prices for this departement, None if departement data is missing
        '''
        choro_appart = folium.Choropleth(
            geo_data=d_geodata,
            fill_color=row_appart["color"] if row_appart is not None else "white",
            fill_opacity=0.5,
            line_opacity=1,
            line_weight=1,
            line_color='blue',
        )
        choro_maison = folium.Choropleth(
            geo_data=d_geodata,
            fill_color=row_maison["color"] if row_maison is not None else "white",
            fill_opacity=0.5,
            line_opacity=1,
            line_weight=1,
            line_color='blue',
        )

        if row_appart is not None:
            popup_appart = DepartementMap.make_line_chart_popup(row_appart, title=d_geodata["features"][0]["properties"]["nom"])
            popup_appart.add_to(choro_appart)
            popup_maison = DepartementMap.make_line_chart_popup(row_maison, title=d_geodata["features"][0]["properties"]["nom"])
            popup_maison.add_to(choro_maison)
        else:
            popup_appart = folium.Popup("Données source manquantes")
            popup_appart.add_to(choro_appart)
            popup_maison = folium.Popup("Données source manquantes")
            popup_maison.add_to(choro_maison)

        choro_appart.add_to(self.fgroup_appart)
        choro_maison.add_to(self.fgroup_maison)

    def save(self, file_path):
        '''Save to html file'''
        # add the color bar to top right of the map
        colormap.add_to(self.map)

        self.fgroup_appart.add_to(self.map)
        self.fgroup_maison.add_to(self.map)

        lcontrol = folium.map.LayerControl(position='topright', collapsed=False)
        lcontrol.add_to(self.map)

        title_image_path = "static/images/title_logo_departement.png"
        # create and save the title image to the path
        create_title_image(self.title, image_path=title_image_path)
        # create Floating image, image will be loaded from the path when map is loaded in prod
        FloatImage(title_image_path, bottom=95, left=30).add_to(self.map)

        self.map.save(file_path)


if __name__ == "__main__":

    df_appart = pd.read_csv("data/immobilier/data_clean/m2_appartement_price_per_departement.csv", index_col=0)
    df_maison = pd.read_csv("data/immobilier/data_clean/m2_maison_price_per_departement.csv", index_col=0)

    # istance of a LinearColormap for departement coloration
    # LinearColormap class was modified from source, this code will not work with branca library from pip 
    # (added index_display property for rendering)
    colormap = cm.LinearColormap(colors=['darkgreen', 'green', 'yellow', 'orange', 'red', 'darkred'],
                             index=[700, 1300, 2000, 2800, 5000, 10000], 
                             index_display=[700, 1500, 2500, 4500, 7000, 10000],
                             vmin=700, vmax=10000,
                             caption="par département en €")

    # normal Colormap class utilisation, work with branca library from pip
    # colormap = cm.LinearColormap(colors=['darkgreen', 'green', 'yellow', 'orange', 'red', 'darkred'],
    #                          index=[700, 1300, 2000, 2800, 5000, 10000], 
    #                          vmin=700, vmax=10000,
    #                          caption="Prix median du m2 par département")


    df_appart["color"] = df_appart["2019_median"].apply(colormap)
    df_maison["color"] = df_maison["2019_median"].apply(colormap)

    longitude, latitude = 45.8566, 2.3522
    map1 = DepartementMap(longitude, latitude, "Prix médian du m2 d'un bien immobilier en France")

    with open("data/immobilier/geo_data/departements.geojson.txt", "r") as file:
        json_departements = json.load(file)

    for departement in json_departements["features"]:
        d_geodata = {"type": "FeatureCollection", "features": [departement]}
        if departement["properties"]["code"] in df_appart.index.tolist():
            map1.draw_departement(d_geodata, df_appart.loc[departement["properties"]["code"],:],
                                df_maison.loc[departement["properties"]["code"],:])
        else:
            map1.draw_departement(d_geodata)

    map1.save(file_path="templates/immobilier/maps/map_departement_folium.html")
