from tqdm.auto import tqdm
import joblib
import arus
import os
import pandas as pd


def load_dataset(root, placement_parser, class_set_parser):
    hh = arus.ds.MHDataset(name='hand_hygiene',
                                path=root)
    hh.set_placement_parser(placement_parser)
    hh.set_class_set_parser(class_set_parser)
    return hh


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


def import_sensor_data(subj):
    sensor_dfs = []
    placements = []
    srs = []
    data_types = []
    sensors = subj.sensors
    for sensor in sensors:
        if sensor.data_type in ['IMUAccelerometerCalibrated', 'IMUMagnetometer', 'IMUGyroscope']:
            sensor.data = arus.mh.MhealthFileReader.read_csvs(
                *sensor.paths)
            sensor_dfs.append(sensor.data)
            placements.append(sensor.placement)
            srs.append(sensor.sr)
            data_types.append(sensor.data_type)
    return sensor_dfs, placements, srs, data_types


def segment_sensor_data(sensor_dfs, st, et):
    dfs = [arus.ext.pandas.segment_by_time(
        df, seg_st=st, seg_et=et) for df in sensor_dfs]
    return dfs


def parse_pids(pids='all', root=None):
    if pids == 'all':
        pids = arus.mh.get_pids(root)
    else:
        parsed_pids = []
        for pid in pids:
            if '_' in pid:
                parsed_pids.append(pid)
            else:
                parsed_pids += [f'{pid}_{i}' for i in range(1, 7)]
        pids = parsed_pids
    return pids


class ProgressParallel(joblib.Parallel):
    def __init__(self, use_tqdm=True, total=None, *args, **kwargs):
        self._use_tqdm = use_tqdm
        self._total = total
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        with tqdm(disable=not self._use_tqdm, total=self._total) as self._pbar:
            return joblib.Parallel.__call__(self, *args, **kwargs)

    def print_progress(self):
        if self._total is None:
            self._pbar.total = self.n_dispatched_tasks
        self._pbar.n = self.n_completed_tasks
        self._pbar.refresh()
