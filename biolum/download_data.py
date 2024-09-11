import requests
import os
import json
from urllib.parse import urlparse
from typing import Dict, List, Any, Tuple
from multiprocessing import Pool, cpu_count
from functools import partial

def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, 'r') as f:
        return json.load(f)

def get_mission(base_url: str, mission_id: str) -> Dict[str, Any]:
    url = f"{base_url}/missions/{mission_id}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_collection_events(base_url: str, mission_id: str) -> List[Dict[str, Any]]:
    url = f"{base_url}/missions/{mission_id}/collectionEvents"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_collection_event(base_url: str, collection_event_id: str) -> Dict[str, Any]:
    url = f"{base_url}/collectionEvents/{collection_event_id}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_data_records(base_url: str, collection_event_id: str) -> List[Dict[str, Any]]:
    url = f"{base_url}/collectionEvents/{collection_event_id}/data"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def filter_data(data_records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Filter data records to separate images and GCOOS CSV files."""
    image_data = []
    csv_data = []
    for record in data_records:
        if record['dataType'] == 'Image' and not record.get('filepath', '').endswith('.CR2'):
            image_data.append(record)
        elif record.get('storageLocation') == 'GCOOS' and record.get('filepath', '').endswith('.csv'):
            csv_data.append(record)
    return image_data, csv_data

def get_presigned_url(base_url: str, default_bucket: str, record: Dict[str, Any]) -> Tuple[str, str]:
    bucket = record.get('bucket', default_bucket)
    key = extract_s3_key(record.get('filepath', ''))
    storage_location = record.get('storageLocation', 'Amazon Web Services')
    
    url = f"{base_url}/presignedurls"
    payload = {
        "bucket": bucket,
        "key": key,
        "storageLocation": storage_location
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return record['_id'], response.json()['url']
    except Exception as e:
        print(f"Error getting presigned URL for record {record['_id']}: {str(e)}")
        return record['_id'], None

def download_and_save(record: Dict[str, Any], url: str, main_directory: str, mission_id: str, collection_event_id: str):
    data_id = record['_id']
    file_extension = os.path.splitext(record.get('filepath', ''))[1]
    
    dir_path = os.path.join(main_directory, mission_id, collection_event_id)
    os.makedirs(dir_path, exist_ok=True)
    
    if not url:
        print(f"No URL available for record {data_id}. Skipping.")
        return
    
    try:
        file_path = os.path.join(dir_path, f"{data_id}{file_extension}")
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            f.write(response.content)

        metadata_path = os.path.join(dir_path, f"{data_id}.json")
        with open(metadata_path, 'w') as f:
            json.dump(record, f, indent=2)
        
        print(f"Processed file: {data_id}")
    except Exception as e:
        print(f"Error processing record {data_id}: {str(e)}")

def extract_s3_key(url: str) -> str:
    parsed_url = urlparse(url)
    return parsed_url.path.lstrip('/')

def process_data(config: Dict[str, Any], image_data: List[Dict[str, Any]], csv_data: List[Dict[str, Any]], mission_id: str, collection_event_id: str):
    num_processes = min(cpu_count(), len(image_data) + len(csv_data))
    
    # get presigned URLs for images in parallel
    get_presigned_url_with_config = partial(get_presigned_url, config['base_url'], config['default_bucket'])
    with Pool(processes=num_processes) as pool:
        presigned_url_results = pool.map(get_presigned_url_with_config, image_data)
    
    presigned_url_dict = dict(presigned_url_results)
    
    # CSV data, for GCOOS missions no presigned URL needed
    csv_url_dict = {record['_id']: record['filepath'] for record in csv_data}
    
    # download and save data
    download_func = partial(download_and_save, main_directory=config['main_directory'], mission_id=mission_id, collection_event_id=collection_event_id)
    with Pool(processes=num_processes) as pool:
        pool.starmap(download_func, [(record, presigned_url_dict.get(record['_id'])) for record in image_data] +
                                    [(record, csv_url_dict.get(record['_id'])) for record in csv_data])

def save_mission_metadata(main_directory: str, mission_id: str, mission_data: Dict[str, Any]):
    file_path = os.path.join(main_directory, f"{mission_id}.json")
    with open(file_path, 'w') as f:
        json.dump(mission_data, f, indent=2)

def save_collection_event_metadata(main_directory: str, mission_id: str, collection_event_id: str, collection_event_data: Dict[str, Any]):
    dir_path = os.path.join(main_directory, mission_id)
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{collection_event_id}.json")
    with open(file_path, 'w') as f:
        json.dump(collection_event_data, f, indent=2)

def process_mission(config: Dict[str, Any], mission_config: Dict[str, Any]):
    mission_id = mission_config['mission_id']
    download_all = mission_config['download_all_collection_events']
    collection_events = mission_config.get('collection_events', [])

    mission = get_mission(config['base_url'], mission_id)
    save_mission_metadata(config['main_directory'], mission_id, mission)
    print(f"Mission Name: {mission['name']}")

    if download_all:
        collection_events = get_collection_events(config['base_url'], mission_id)
    
    for collection_event in collection_events:
        if download_all:
            collection_event_id = collection_event['_id']
        else:
            collection_event_id = collection_event
            collection_event = get_collection_event(config['base_url'], collection_event_id)
        
        save_collection_event_metadata(config['main_directory'], mission_id, collection_event_id, collection_event)
        collection_name = collection_event.get('name', 'unknown_collection')
        print(f"Processing Collection: {collection_name}")

        data_records = get_data_records(config['base_url'], collection_event_id)
        print(f"Total Number of Data Records: {len(data_records)}")

        image_data, csv_data = filter_data(data_records)
        print(f"Number of Image Data Records: {len(image_data)}")
        print(f"Number of CSV Data Records: {len(csv_data)}")

        process_data(config, image_data, csv_data, mission_id, collection_event_id)

def main():
    config = load_config('config.json')
    
    os.makedirs(config['main_directory'], exist_ok=True)

    for mission_config in config['missions']:
        process_mission(config, mission_config)

    print(f"All missions processed. See '{config['main_directory']}/'")

if __name__ == "__main__":
    main()