import arus
import os
import pandas as pd


def _load_placement_map(filepath):
    if os.path.exists(filepath):
        return pd.read_csv(filepath)
    else:
        return None


def get_placement(root, pid, sid):
    placement_map_file = os.path.join(root, pid, arus.mh.SUBJECT_META_FOLDER,
                                      arus.mh.META_LOCATION_MAPPING_FILENAME)
    placement_map = _load_placement_map(placement_map_file)
    if placement_map is None:
        return None
    else:
        p = placement_map.loc[placement_map['SENSOR_ID']
                              == sid, 'PLACEMENT'].values[0]
        return p
