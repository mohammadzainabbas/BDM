from flask import Flask 
import folium
from folium.plugins import Draw
from folium.plugins import MeasureControl
from folium.plugins import MousePosition
from folium.plugins import fullscreen
import os

app = Flask(__name__)


@app.route('/')
def index():
    start_coords = (-15.79340,-47.88232)
    folium_map = folium.Map(location=start_coords, zoom_start=16)
    draw = Draw()
    draw.add_to(folium_map)
    folium_map.add_child(MeasureControl())
    tooltip = 'Click me!'
    folium.Marker(
    [-15.796218,-47.872002 ], 
    popup='<i>Estrada Parque Ceilândia, SIA</i>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.791077,-47.875543], 
    popup='<b>QSB 12, Taguatinga, Região</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.787711,-47.878397], 
    popup='<b>Bloco A / B, W1 Sul, SQS 104, Asa Sul</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.790932,-47.879212], 
    popup='<b>Recanto das Emas, Região Integrada de Desenvol...</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.793678,-47.880199], 
    popup='<b>Recanto das Emas, Região Integrada de Desenvol...</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.79628,-47.885885], 
    popup='<b>Recanto das Emas, Região Integrada de Desenvol...</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.789507,-47.883203], 
    popup='<b>Recanto das Emas, Região Integrada de Desenvol...</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-47.887988,-47.887988], 
    popup='<b>Recanto das Emas, Região Integrada de Desenvol...</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.79436,-47.887602], 
    popup='<b>Recanto das Emas, Região Integrada de Desenvol...</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.788743,-47.889705], 
    popup='<b>Recanto das Emas, Região Integrada de Desenvol...</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    folium.Marker(
    [-15.788826,-47.89346], 
    popup='<b>Recanto das Emas, Região Integrada de Desenvol...</b>', 
    icon=folium.Icon(color='black',icon='car', prefix='fa'),
    tooltip=tooltip).add_to(folium_map)
    formatter = "function(num) {return L.Util.formatNum(num, 3) + ' º ';};"
    MousePosition(
    position='topright',
    separator=' | ',
    empty_string='NaN',
    lng_first=True,
    num_digits=20,
    prefix='Coordinates:',
    lat_formatter=formatter,
    lng_formatter=formatter,
    ).add_to(folium_map)
    return folium_map._repr_html_()


if __name__ == '__main__':
    app.run(debug=True, port=4000)