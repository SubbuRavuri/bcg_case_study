import json

def get_output_path(output_config_path, key):
    with open(output_config_path, 'r') as f:
        paths = json.load(f)
    return paths.get(key)
