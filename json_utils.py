from __future__ import annotations

from datetime import date, datetime

import numpy as np


def json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if np.issubdtype(type(obj), np.integer):
        return int(obj)
    if np.issubdtype(type(obj), np.floating):
        return float(obj)
    return str(obj)
