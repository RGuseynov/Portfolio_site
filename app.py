from flask import Flask, request, url_for, render_template, flash, redirect
from flask_wtf import FlaskForm, CSRFProtect
from wtforms import StringField, TextField, SubmitField, IntegerField, RadioField 
from wtforms.validators import DataRequired, Length, NumberRange, ValidationError

import os
import pickle


class InferenceForm(FlaskForm):
    """Inference form"""
    type_bien = RadioField(
        'Type du bien', validators=[DataRequired(message=("Choisissez un champ"))], 
        choices=[('appartement','appartement'),('maison','maison')])
    surface = IntegerField(
        'Surface habitable en m2', validators=[DataRequired(message="Entrez un nombre"), 
        NumberRange(min=9, max=500, message="Doit être compris entre 9 et 500")])
    nb_pieces = IntegerField(
        'Nombre de pièces principales', validators=[DataRequired(message="Entrez un nombre"), 
        NumberRange(min=1, max=50, message="Doit être compris entre 1 et 50")])
    code_postal = IntegerField(
        "Code postal", validators=[DataRequired(message="Entrez un nombre"), 
        NumberRange(min=1000, max=98000, message="Doit être compris entre 1000 et 98000")])
    submit = SubmitField('Prédire')


# inference fonction, parameters have passed form filters before reaching this function
# still user can input inexistant code postal (verification could be implimented from flat file or db)
def make_inference(type_bien, surface, nb_pieces, code_postal):
    if type_bien == "appartement":
        with open("ml_models/tree_appartement.pkl", "rb") as file:
            tree = pickle.load(file)
        prediction = tree.predict([[surface, nb_pieces, code_postal]])
    else:
        with open("ml_models/tree_maison.pkl", "rb") as file:
            tree = pickle.load(file)
        prediction = tree.predict([[surface, nb_pieces, code_postal]])
    return prediction[0]


csrf = CSRFProtect()
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

@app.route('/about', methods=['GET'])
def about():
    return render_template('about.html')

@app.route('/immobilier/presentation', methods=['GET'])
def immo_presentation():
    return render_template('immobilier/presentation.html')

@app.route('/immobilier/estimation', methods=['GET', 'POST'])
def immo_estimation():
    form = InferenceForm()
    if form.validate_on_submit():
        type_bien = form.type_bien.data
        surface = form.surface.data
        nb_pieces = form.nb_pieces.data
        code_postal = form.code_postal.data
        result = round(make_inference(type_bien, surface, nb_pieces, code_postal))
        return render_template('immobilier/predictions/resultat_estimation.html', result=result)
    else:
        print(form.errors.items())
    return render_template('immobilier/predictions/estimation.html', form=form)

#page that contain map
@app.route('/immobilier/map_departement', methods=['GET'])
def immo_map_departement():
    map_name = "map_departement_folium"
    return render_template('immobilier/maps/map_departement.html', map_name=map_name)

#only the folium map, it will be rendered inside map_departement
@app.route('/immobilier/show_map', methods=['GET'])
def immo_show_map():
    map_name = request.args.get("map_name")
    return render_template(f'immobilier/maps/{map_name}.html')
# @app.route('/show_map/<map_name>', methods=['GET'])
# def show_map(map_name):
#     return render_template(f'immobilier/maps/{map_name}.html')

# DVF_2019_raport_initial
@app.route('/immobilier/data_exploration', methods=['GET'])
def immo_data_exploration():
    data = {"notebook1_name": "immobilier_EDA_1",
            "notebook1_height" : 2680,
            "report1_name": "DVF_2019_raport_initial",
            "report1_height" : 17560}
    return render_template('immobilier/data_exploration/data_exploration.html', data=data)

@app.route('/immobilier/show_notebook/<notebook>')
def immo_show_notebook(notebook):
    return render_template(f'immobilier/notebooks/{notebook}.html')


@app.route('/immobilier/pandas_profiling')
def immo_pandas_profiling():
    report_name = request.args.get("report")
    report_height = request.args.get("height")
    report = {"name": report_name,
              "height": report_height}
    return render_template(f"immobilier/data_exploration/pandas_profiling.html", report=report)

@app.route('/immobilier/show_pandas_profiling')
def immo_show_pandas_profiling():
    report_name = request.args.get("report_name")
    return render_template(f'immobilier/data_exploration/{report_name}.html')



@app.route('/climat/presentation')
def cl_presentation():
    return render_template('climat/presentation.html')

@app.route('/climat/specifications_donnees')
def cl_specifications_donnees():
    return render_template("climat/specifications_donnees.html")

@app.route('/climat/traitement_donnees')
def cl_traitement_donnees():
    data = {"notebook1_name": "traitement_donnees",
            "notebook1_height" : 13290}
    return render_template("climat/traitement_donnees.html", data=data)

@app.route('/climat/clusterisation')
def cl_clusterisation():
    data = {"notebook1_name": "clusterisation",
            "notebook1_height" : 10640}
    return render_template("climat/clusterisation.html", data=data)

@app.route('/climat/modelisation_pbi')
def cl_modelisation_pbi():
    return render_template("climat/modelisation_pbi.html")

@app.route('/climat/rapport_power_bi')
def cl_rapport_power_bi():
    return render_template("climat/rapport_power_bi.html")

@app.route('/climat/show_notebook/<notebook>')
def cl_show_notebook(notebook):
    return render_template(f'climat/notebooks/{notebook}.html')

