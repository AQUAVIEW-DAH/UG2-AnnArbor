import os
import json
from flask import Flask, Response
import folium
from PIL import Image
import io
import base64
import random
import pandas as pd

app = Flask(__name__)

CACHE_DIR = "aquaviewcache"

def compress_image(image_path, max_size=(800, 800)):
    try:
        with Image.open(image_path) as img:
            img.thumbnail(max_size)
            buffered = io.BytesIO()
            img.save(buffered, format="JPEG", quality=85)
        print(f"Successfully compressed image: {image_path}")
        return base64.b64encode(buffered.getvalue()).decode('utf-8')
    except Exception as e:
        print(f"Error processing image {image_path}: {str(e)}")
        return None

def process_csv(csv_path):
    df = pd.read_csv(csv_path)
    columns_of_interest = ['time', 'latitude', 'longitude', 'depth', 'temperature', 'salinity', 'pressure', 'conductivity']
    df = df[columns_of_interest]
    
    # About 200 data points for each glider dataset
    group_factor = max(1, len(df) // 200)
    grouped = df.groupby(df.index // group_factor).mean(numeric_only=True)
    
    print(f"CSV {csv_path}: Original points: {len(df)}, Reduced to: {len(grouped)}")
    
    return [
        {
            'lat': row['latitude'],
            'lon': row['longitude'],
            'csv_data': row.to_dict()
        }
        for _, row in grouped.iterrows()
    ]

def load_data():
    all_data = []
    csv_data_sets = []
    for mission_dir in os.listdir(CACHE_DIR):
        mission_path = os.path.join(CACHE_DIR, mission_dir)
        if os.path.isdir(mission_path):
            for collection_dir in os.listdir(mission_path):
                collection_path = os.path.join(mission_path, collection_dir)
                if os.path.isdir(collection_path):
                    for file in os.listdir(collection_path):
                        file_path = os.path.join(collection_path, file)
                        if file.lower().endswith(('.png', '.jpg', '.jpeg')):
                            print(f"Found image file: {file_path}")
                            json_file = os.path.splitext(file)[0] + '.json'
                            json_path = os.path.join(collection_path, json_file)
                            if os.path.exists(json_path):
                                print(f"Found corresponding JSON: {json_path}")
                                with open(json_path, 'r') as f:
                                    metadata = json.load(f)
                                if 'lat' in metadata and 'lon' in metadata:
                                    # Not enough significant digits in metadata, uniform
                                    offset = 0.01
                                    lat = metadata['lat'] + random.uniform(-offset, offset)
                                    lon = metadata['lon'] + random.uniform(-offset, offset)
                                    all_data.append({
                                        'type': 'image',
                                        'id': metadata.get('_id', file),
                                        'lat': lat,
                                        'lon': lon,
                                        'image_path': file_path,
                                        'metadata': metadata,
                                        'mission': mission_dir
                                    })
                                else:
                                    print(f"Missing lat or lon in JSON: {json_path}")
                            else:
                                print(f"No corresponding JSON found for: {file_path}")
                        elif file.endswith('.csv'):
                            csv_path = os.path.join(collection_path, file)
                            print(f"Processing CSV file: {csv_path}")
                            csv_data = process_csv(csv_path)
                            csv_data_sets.append({
                                'mission': mission_dir,
                                'file_name': file,
                                'data': csv_data
                            })
                            all_data.extend([{'type': 'csv', 'mission': mission_dir, 'file_name': file, **item} for item in csv_data])
                            print(f"Loaded CSV data: {csv_path}, {len(csv_data)} points")
    print(f"Total data points loaded: {len(all_data)}")
    return all_data, csv_data_sets

@app.route('/')
def map_view():
    data, csv_data_sets = load_data()
    
    if not data:
        return "No data found or all data points are invalid."

    center_lat = sum(item['lat'] for item in data) / len(data)
    center_lon = sum(item['lon'] for item in data) / len(data)

    m = folium.Map(location=[center_lat, center_lon], zoom_start=6)

    # Create a color map for CSV datasets
    csv_colors = ['red', 'green', 'blue', 'purple', 'orange', 'darkred', 'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'pink', 'lightblue', 'lightgreen']
    color_map = {dataset['file_name']: csv_colors[i % len(csv_colors)] for i, dataset in enumerate(csv_data_sets)}

    for item in data:
        if item['type'] == 'image':
            img_data = compress_image(item['image_path'])
            if img_data:
                popup_content = f"""
                <div style="font-family: Arial, sans-serif;">
                <b>Image Data</b><br>
                Mission: {item['mission']}<br>
                Latitude: {item['lat']:.6f}<br>
                Longitude: {item['lon']:.6f}<br>
                <img src="data:image/jpeg;base64,{img_data}" alt="Image" style="max-width:100%;"/><br>
                <b>Metadata:</b><br>
                <pre>{json.dumps(item['metadata'], indent=2, sort_keys=True)}</pre>
                </div>
                """
                iframe = folium.IFrame(popup_content, width=500, height=500)
                popup = folium.Popup(iframe, max_width=500)
                folium.Marker(
                    [item['lat'], item['lon']],
                    popup=popup,
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
            else:
                print(f"Failed to load image: {item['image_path']}")
        else:
            popup_content = f"""
            <div style="font-family: Arial, sans-serif;">
            <b>Sensor Data</b><br>
            Mission: {item['mission']}<br>
            File: {item['file_name']}<br>
            <pre>{json.dumps(item['csv_data'], indent=2)}</pre>
            </div>
            """
            iframe = folium.IFrame(popup_content, width=400, height=180)
            popup = folium.Popup(iframe, max_width=400)
            folium.CircleMarker(
                [item['lat'], item['lon']],
                radius=3,
                popup=popup,
                color=color_map[item['file_name']],
                fill=True,
                fillColor=color_map[item['file_name']]
            ).add_to(m)

    # Add lines connecting CSV data points for each dataset
    for dataset in csv_data_sets:
        coordinates = [(item['lat'], item['lon']) for item in dataset['data']]
        folium.PolyLine(
            coordinates,
            color=color_map[dataset['file_name']],
            weight=2,
            opacity=0.8
        ).add_to(m)

    return Response(m.get_root().render(), content_type='text/html')

if __name__ == '__main__':
    app.run(debug=False)