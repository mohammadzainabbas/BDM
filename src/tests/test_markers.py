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
    # start_coords = (-15.79340,-47.88232)
    # start_coords = (41.3927755,2.0701489)
    start_coords, zoom_start = (41.389, 2.15), 14
    folium_map = folium.Map(location=start_coords, zoom_start=zoom_start)
    draw = Draw()
    draw.add_to(folium_map)
    folium_map.add_child(MeasureControl())
    tooltip = 'Click me!'

    folium.Marker([41.376204, 2.1198613], popup='<i>Barcelona Accomodation</i>', icon=folium.Icon(color='black',icon='home', prefix='fa'), tooltip=tooltip).add_to(folium_map)
    folium.Marker([41.3751105,2.1027038], popup='<i>Kainaat Place</i>', icon=folium.Icon(color='black',icon='home', prefix='fa'), tooltip=tooltip).add_to(folium_map)
    folium.Marker([41.3873154,2.1112131], popup='<i>Tejaswini Place</i>', icon=folium.Icon(color='black',icon='home', prefix='fa'), tooltip=tooltip).add_to(folium_map)
    
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