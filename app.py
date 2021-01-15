from flask import Flask, request, url_for, render_template, flash
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
        'Surface habitable', validators=[DataRequired(message="Entrez un nombre"), 
        NumberRange(min=9, max=500, message="Doit être compris entre 9 et 500")])
    nb_pieces = IntegerField(
        'Nombre de pièces principales', validators=[DataRequired(message="Entrez un nombre"), 
        NumberRange(min=1, max=50, message="Doit être compris entre 1 et 50")])
    code_postal = IntegerField(
        "Code postal", validators=[DataRequired(message="Entrez un nombre"), 
        NumberRange(min=1000, max=98000, message="Doit être compris entre 1000 et 98000")])
    submit = SubmitField('Submit')


# inference fonction, contains the transformation for Corse code for the model too
# parameters have passed form filters before reaching this function
# still user can input inexistant code commun (verification could be implimented from flat file or db)
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


@app.route('/estimation', methods=['GET', 'POST'])
def estimation():
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


#page that extend the layout and contain map
@app.route('/map_departement_page', methods=['GET'])
def map_departement_page():
    map_name = "map_departement_folium"
    return render_template('immobilier/maps/map_departement_page.html', map_name=map_name)

#only the folium map, it will be rendered inside another page
@app.route('/show_map', methods=['GET'])
def show_map():
    map_name = request.args.get("map_name")
    return render_template(f'immobilier/maps/{map_name}.html')
# @app.route('/show_map/<map_name>', methods=['GET'])
# def show_map(map_name):
#     return render_template(f'immobilier/maps/{map_name}.html')

# DVF_2019_raport_initial
@app.route('/data_exploration_page', methods=['GET'])
def data_exploration_page():
    data = {"notebook1_name": "immobilier_EDA_1",
            "notebook1_height" : 2680,
            "report1_name": "DVF_2019_raport_initial",
            "report1_height" : 17560}
    return render_template('immobilier/data_exploration/data_exploration_page.html', data=data)

@app.route('/show_notebook/<notebook>')
def show_notebook(notebook):
    return render_template(f'immobilier/notebooks/{notebook}.html')


@app.route('/pandas_profiling_page')
def pandas_profiling_page():
    report_name = request.args.get("report")
    report_height = request.args.get("height")
    report = {"name": report_name,
              "height": report_height}
    return render_template(f"immobilier/data_exploration/pandas_profiling_page.html", report=report)

@app.route('/show_pandas_profiling')
def show_pandas_profiling():
    report_name = request.args.get("report_name")
    return render_template(f'immobilier/data_exploration/{report_name}.html')

