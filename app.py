from flask import Flask, request, url_for, render_template, flash
from flask_wtf import FlaskForm, CSRFProtect
from wtforms import StringField, TextField, SubmitField, IntegerField
from wtforms.validators import DataRequired, Length, NumberRange, ValidationError

import os
import pickle
import re


class InferenceForm(FlaskForm):
    """Inference form"""
    surface = IntegerField('Surface', validators=[DataRequired(message="Entrez un nombre"), NumberRange(min=9, max=250)])
    nb_pieces = IntegerField('Nombre de pièces', validators=[DataRequired(message="Entrez un nombre"), NumberRange(min=1, max=50)])
    code_commune = StringField("Code commune (Différent du code postal)", validators=[DataRequired(), Length(min=4, max=5)])
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
        surface = form.surface.data
        nb_pieces = form.nb_pieces.data
        code_commune = form.code_commune.data
        result = round(make_inference(surface, nb_pieces, code_commune))
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
@app.route('/show_map/<map_name>', methods=['GET'])
def show_map(map_name):
    return render_template(f'immobilier/maps/{map_name}.html')

# DVF_2019_raport_initial
@app.route('/data_exploration_page', methods=['GET'])
def data_exploration_page():
    data = {"notebook1_name": "immobilier_EDA_1",
            "notebook1_height" : 2680,
            "report1_name": "DVF_2019_raport_initial",
            "report1_height" : 13000}
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

