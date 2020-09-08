import json

def load_json(filename):
    try:
        with open(filename) as fp:
            mapper = json.load(fp)
    except Exception as e:
        return None
    else:
        return mapper


