from flask import Flask
from keplergl import KeplerGl
import pandas as pd
import requests
import random

app = Flask(__name__)


API_URL = "https://wfh1llc30j.execute-api.us-east-2.amazonaws.com/missions?limit=0&skip=0"

def fetch_missions():
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return []

def random_color():
    return [random.randint(0, 255) for _ in range(3)]

missions = fetch_missions()
map_1 = KeplerGl()


for mission in missions:
    df = pd.DataFrame([mission])
    map_1.add_data(data=df, name=mission['name'])

config = {
    "version": "v1",
    "config": {
        "visState": {
            "layers": []
        },
        "mapState": {
            "latitude": 28,
            "longitude": -92,
            "zoom": 5
        }
    }
}

config["config"]["visState"]["layers"] = [
    {
        "type": "point",
        "config": {
            "dataId": mission['name'],
            "label": mission['name'],
            "color": random_color(),
            "columns": {
                "lat": "lat",
                "lng": "lon",
                "altitude": None
            },
            "isVisible": True,
            "visConfig": {
                "radius": 6,
                "fixedRadius": False,
                "opacity": 0.8,
                "outline": True,
                "thickness": 2,
                "strokeColor": random_color(),
                "strokeOpacity": 0.8
            }
        }
    } for mission in missions
]

map_1.config = config

@app.route('/')
def index():
    return map_1._repr_html_()

if __name__ == '__main__':
    app.run(debug=True)