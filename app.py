from flask import Flask, request, url_for, render_template, flash
from flask_wtf import FlaskForm
from flask_wtf.csrf import CsrfProtect
from wtforms import StringField, TextField, SubmitField, IntegerField
from wtforms.validators import DataRequired, Length, NumberRange, ValidationError

import os
import pickle
import re


class InferenceForm(FlaskForm):
    """Inference form"""
    surface = IntegerField('Surface', validators=[DataRequired(message="Entrez un nombre"), NumberRange(min=9, max=250)])
    nb_pieces = IntegerField('Nombre de pièces', validators=[DataRequired(message="Entrez un nombre"), NumberRange(min=1, max=50)])
    code_commune = StringField("Code commune", validators=[DataRequired(), Length(min=4, max=5)])
    submit = SubmitField('Submit')

    def validate_code_commune(self, code_commune):
        re_pattern = "^(2[AB]?|\d?\d)\d{3}$"
        if not re.match(re_pattern, self.code_commune.data):
            raise ValidationError("Mauvais format")


# inference fonction, contains the transformation for Corse code for the model too
# parameters have passed form filters before reaching this function
# still user can input inexistant code commun (verification could be implimented from flat file or db)
def make_inference(surface, nb_pieces, code_commune):
    with open("ml_models/tree1.pkl", "rb") as file:
        tree1 = pickle.load(file)
    try:
        code_commune = int(code_commune)
    except:
        if code_commune[1] == "A":
            code_commune = int(code_commune[-3:]) + 100000
        elif code_commune[1] == "B":
            code_commune = int(code_commune[-3:]) + 110000
    prediction = tree1.predict([[surface, nb_pieces, code_commune]])
    return prediction[0]


csrf = CsrfProtect()
app = Flask(__name__)
try:
    app.config.from_pyfile('config_dev.py')
except:
    app.config['SECRET_KEY'] = os.environ["FLASK_KEY"]
csrf.init_app(app)

@app.route('/', methods=['GET'])
@app.route('/home', methods=['GET'])
def home():
    return render_template('home.html')


@app.route('/coordonnee', methods=['GET', 'POST'])
def coordonnee():
    if request.method == 'POST':
        # arrondissement = plus_proche_arrondissement(float(request.form["longitude"]), float(request.form["latitude"]), json_arr)
        # variation = conversion_pourcentage(variation_2014_2018.loc[[int(arrondissement)], ["Variation"]].values[0][0])
        return f"Vous habitez dans l'arrondissement {arrondissement}, le prix y a varier de: {variation}% entre 2014 et 2018"
    return '<form action="" method="post">Longitude: <input type="text" name="longitude" /> Latitude: <input type="text" name="latitude" /><input type="submit" value="Envoyer" /></form>'


#only the folium map, it will be renderer inside map_page_appartement.html
@app.route('/map_folium_appartement', methods=['GET'])
def render_appartement_map():
    return render_template('immobilier/map_folium_appartement.html')

#page that extend the layout and contain the folium map
@app.route('/map_page_appartement', methods=['GET'])
def map_page_appartement():
    return render_template('immobilier/map_page_appartement.html')


@app.route('/estimation', methods=['GET', 'POST'])
def estimation():
    form1 = InferenceForm()
    if form1.validate_on_submit():
        surface = form1.surface.data
        nb_pieces = form1.nb_pieces.data
        code_commune = form1.code_commune.data
        result = make_inference(surface, nb_pieces, code_commune)
        result = round(result)
        print(result)
        return render_template('immobilier/resultat_estimation.html', data=result)
    else:
        print(form1.errors.items())
    return render_template('immobilier/estimation.html', form=form1)


@app.route('/indisponible', methods=['GET'])
def indisponible():
    return render_template("vide.html") 

