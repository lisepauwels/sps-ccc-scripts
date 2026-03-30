import json
from pathlib import Path
from xjson import NumpyFriendlyEncoder

def store_data(name, data, meta={}, header=None):
    name = name.split('+')[0]
    data = clean_bbq_data(data)
    data = clean_bct_data(data)
    res = {**meta, 'header': {}, 'data': data}
    if header is None:
        res.pop('header', None)
    else:
        res['header'] = header
    Path(f'results/{name}.json').parent.mkdir(parents=True, exist_ok=True)
    with Path(f'results/{name}.json').open("w") as fp:
        json.dump(res, fp, indent=4, cls=NumpyFriendlyEncoder)

def result_exists(name=None):
    if name is None:
        raise ValueError("Name must be provided to check for result existence.")
    return Path(f'results/{name}.json').exists()

def clean_bbq_data(result):
    result.pop('rawDataQ', None)
    result.pop('exDataH', None)
    result.pop('exDataV', None)
    return result

def clean_bct_data(result):
    result.pop('bTrain', None)
    result.pop('range1Data', None)
    result.pop('range3Data', None)
    return result