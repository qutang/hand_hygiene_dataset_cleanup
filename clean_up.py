import datetime
import glob
import json
import os
import shutil

import arus
from arus.extensions.pandas import segment_by_time
import pandas as pd
from pyarrow.feather import read_feather
import tqdm
from loguru import logger
import pyarrow.feather as feather
import joblib

import correct_orientation as ori
import helpers
import sync_sensor as sync

HAND_CLAPPING_TIME_OFFSETS = [
    (datetime.timedelta(seconds=1, milliseconds=260),
     datetime.timedelta(seconds=1, milliseconds=480)),
    (datetime.timedelta(seconds=2, milliseconds=40),
     datetime.timedelta(seconds=2, milliseconds=380)),
    (datetime.timedelta(seconds=2, milliseconds=860),
     datetime.timedelta(seconds=3, milliseconds=200)),
    (datetime.timedelta(seconds=3, milliseconds=680),
     datetime.timedelta(seconds=3, milliseconds=960)),
    (datetime.timedelta(seconds=4, milliseconds=500),
     datetime.timedelta(seconds=4, milliseconds=800)),
    (datetime.timedelta(seconds=5, milliseconds=380),
     datetime.timedelta(seconds=5, milliseconds=660))
]


def clean_up(root, pid, sr, skip_sync=False, remove_exists=True, date_range=None, auto_range='W-MON'):
    log_file = arus.mh.get_subject_log(root, pid, 'cleanup.log')
    if os.path.exists(log_file):
        os.remove(log_file)
    handle_id = logger.add(log_file)
    convert_to_mhealth(root, pid, skip_sync=skip_sync,
                       remove_exists=remove_exists)
    arus.cli.convert_to_signaligner_both(
        root, pid, sr, date_range=date_range, auto_range=auto_range)
    logger.remove(handle_id)


def convert_to_mhealth(root, pid, skip_sync=False, correct_orientation=True, remove_exists=True):
    master_folder = os.path.join(root, pid, arus.mh.MASTER_FOLDER)
    if remove_exists and os.path.exists(master_folder):
        logger.info(f'Remove existing {master_folder}')
        shutil.rmtree(master_folder)
    annot_df, task_annot_df = _convert_annotations(root, pid)
    _convert_sensors(root, pid,
                     data_type='AccelerometerCalibrated', correct_orientation=correct_orientation,
                     skip_sync=skip_sync, annot_df=annot_df, task_annot_df=task_annot_df)
    _convert_sensors(root, pid, data_type='IMUTenAxes',
                     skip_sync=skip_sync,
                     correct_orientation=correct_orientation,
                     annot_df=annot_df, task_annot_df=task_annot_df)


def _convert_sensors(root, pid, data_type,
                     correct_orientation=True,
                     skip_sync=True, annot_df=None, task_annot_df=None):
    logger.info(
        f"Convert {data_type} data to mhealth format for hand hygiene raw dataset")

    if data_type == 'AccelerometerCalibrated':
        filename_pattern = '*RAW.*'
    elif data_type == 'IMUTenAxes':
        filename_pattern = '*IMU.*'
    else:
        raise NotImplementedError(
            f'The data type {data_type} is not supported')

    master_pid = pid.split('_')[0]
    if master_pid in ['P19']:
        sensor_files = glob.glob(os.path.join(
            root, 'OriginalRawCrossParticipants', master_pid, filename_pattern))
    else:
        sensor_files = glob.glob(os.path.join(
            root, pid, "OriginalRaw", filename_pattern), recursive=True)

    sensor_files = list(
        filter(lambda f: 'csv' in f or 'feather' in f, sensor_files))

    for sensor_file in sensor_files:

        logger.info('Convert {} to mhealth'.format(sensor_file))

        sensor_df, meta = _read_raw_actigraph(sensor_file)
        if not skip_sync and annot_df is not None and task_annot_df is not None:
            logger.info(
                "Start synchronizing this sensor to annotations based on hand clappings")

            peak_cache_path = os.path.join(
                root, pid, 'Cache', os.path.basename(sensor_file).replace('.csv', '.peak').replace('feather', 'peak'))
            if os.path.exists(peak_cache_path):
                logger.info("Loading peak cache...")
                with open(peak_cache_path, 'r') as f:
                    peak_kwargs = json.load(f)
            else:
                peak_kwargs = {
                    'height': -1,
                    'x_height': -1,
                    'y_height': -1,
                    'z_height': -1,
                    'distance': 20,
                    'init_offset': None,
                    'use_axs': 'all'
                }

            session_sensor_df = cache_session(sensor_df, task_annot_df)
            session_sensor_df.to_csv(os.path.join(
                root, pid, "OriginalRaw", os.path.basename(sensor_file)), index=False)

            session_sensor_df, peak_kwargs = sync._sync(
                session_sensor_df, annot_df, task_annot_df, pid=pid, sid=meta['SENSOR_ID'], data_type=data_type, **peak_kwargs)
            st = annot_df.iloc[0, 1] - pd.Timedelta(10, unit='second')
            et = annot_df.iloc[-1, 2] + pd.Timedelta(10, unit='second')

            logger.info(f'Current session: {st} - {et}')
            sensor_df = arus.ext.pandas.segment_by_time(
                session_sensor_df, seg_st=st, seg_et=et)

            # # remove average for magnetometers
            # if data_type == 'IMUTenAxes':
            #     logger.info(f'Removing average for magnetometer sensors...')
            #     sensor_df = _remove_average_for_mag(sensor_df)

            # cache peak keywords
            logger.info('Caching peak parameters...')
            os.makedirs(os.path.dirname(peak_cache_path), exist_ok=True)
            with open(peak_cache_path, 'w') as f:
                json.dump(peak_kwargs, f)

        else:
            logger.warning("Skip synchronization")

        # correct orientations
        p = helpers.get_placement(root, pid, meta['SENSOR_ID'])
        if p is not None and correct_orientation:
            sensor_df = ori.correct(sensor_df, annot_df, p)

        if data_type == 'IMUTenAxes':
            _write_to_mhealth(root, pid, sensor_df, meta,
                              'IMUAccelerometerCalibrated')
            _write_to_mhealth(root, pid, sensor_df, meta,
                              'IMUTemperature')
            _write_to_mhealth(root, pid, sensor_df, meta,
                              'IMUGyroscope')
            _write_to_mhealth(root, pid, sensor_df, meta,
                              'IMUMagnetometer')
        else:
            _write_to_mhealth(root, pid, sensor_df, meta, data_type)


def cache_session(sensor_df, task_annot_df):
    st = task_annot_df.iloc[0, 0] - pd.Timedelta(1, unit='hour')
    et = task_annot_df.iloc[-1, 0] + pd.Timedelta(1, unit='hour')
    sensor_df = arus.ext.pandas.segment_by_time(
        sensor_df, seg_st=st, seg_et=et)
    return sensor_df


def _remove_average_for_mag(sensor_df):
    col_names = arus.mh.parse_column_names_from_data_type('IMUMagnetometer')
    for col_name in col_names:
        sensor_df.loc[:, col_name] = sensor_df.loc[:,
                                                   col_name] - sensor_df.loc[:, col_name].mean()
    return sensor_df


def _read_raw_actigraph(sensor_file):
    cache_file = sensor_file.replace("csv", "feather")
    cache_meta_file = cache_file.replace("feather", "meta")
    if os.path.exists(cache_file):
        sensor_df = feather.read_feather(cache_file)
        meta = joblib.load(cache_meta_file)
    else:
        reader = arus.plugins.actigraph.ActigraphReader(sensor_file)
        read_iterator = reader.read(chunksize=None)
        meta = reader.get_meta()
        sensor_df = next(read_iterator.get_data())
        # cache this as feather object
        feather.write_feather(sensor_df, cache_file)
        joblib.dump(meta, cache_meta_file)
    return sensor_df, meta


def _write_to_mhealth(root, pid, sensor_df, meta, data_type):
    writer = arus.mh.MhealthFileWriter(
        root, pid, hourly=True, date_folders=True)
    writer.set_for_sensor("ActigraphGT9X", data_type,
                          meta['SENSOR_ID'], version_code=meta['VERSION_CODE'].replace('.', ''))
    col_names = arus.mh.parse_column_names_from_data_type(data_type)
    col_names, missing_col_names = filter_exist_cols(
        col_names, sensor_df.columns)
    if len(missing_col_names) > 0:
        logger.warning(
            f'{missing_col_names} column does not exist in {data_type}, skip writing them to files.')
    if len(col_names) > 0:
        chunk_with_selected_cols = sensor_df.loc[:, [
            arus.mh.TIMESTAMP_COL] + col_names]
        writer.write_csv(chunk_with_selected_cols, append=False, block=True)


def filter_exist_cols(col_names, columns):
    return list(filter(lambda name: name in columns, col_names)), list(filter(lambda name: name not in columns, col_names))


def _convert_annotations(root, pid):
    logger.info(
        "Convert annotation data to mhealth format for hand hygiene raw dataset")
    raw_annotation_files = glob.glob(os.path.join(
        root, pid, "OriginalRaw", "**", "*annotations.csv"), recursive=True)
    app_annotation_files = list(filter(
        lambda f: 'Side' not in f, raw_annotation_files))

    annot_dfs = []
    task_annot_dfs = []
    with tqdm.tqdm(total=len(app_annotation_files)) as bar:
        for raw_annotation_file in app_annotation_files:
            bar.update()
            bar.set_description(
                'Convert {} to mhealth'.format(raw_annotation_file))
            if 'openset' in raw_annotation_file:
                annot_df = pd.read_csv(
                    raw_annotation_file, parse_dates=[0, 1])
                annot_df.insert(0, 'HEADER_TIME_STAMP', annot_df.iloc[:, 0])
                annot_df = annot_df.rename(columns={'LABEL': 'LABEL_NAME'})
                annot_df = annot_df.rename(
                    columns={'PREDICTION': 'LABEL_NAME'})
                annot_df = annot_df.iloc[:, :4]
            else:
                annot_df, task_annot_df = _read_raw_annotation_file(
                    raw_annotation_file)
                task_annot_dfs.append(task_annot_df)
            annot_dfs.append(annot_df)

    if len(annot_dfs) > 0:
        annot_df = pd.concat(annot_dfs, axis=0, ignore_index=True)
        annot_df.sort_values(by=annot_df.columns[0], inplace=True)
    else:
        annot_df = None
    if len(task_annot_dfs) > 0:
        task_annot_df = pd.concat(task_annot_dfs, axis=0, ignore_index=True)
        task_annot_df.sort_values(by=task_annot_df.columns[0], inplace=True)
    else:
        task_annot_df = None

    if annot_df is not None:
        if annot_df.iloc[0, 0] >= pd.Timestamp("2020-11-01"):
            # Shift due to day-saving time
            annot_df.iloc[:, [0, 1, 2]] = annot_df.iloc[:,
                                                        [0, 1, 2]] + pd.Timedelta(1, 'hour')
        writer = arus.mh.MhealthFileWriter(
            root, pid, hourly=True, date_folders=True)
        writer.set_for_annotation("HandHygiene", "App")
        writer.write_csv(annot_df, append=False, block=True)
    if task_annot_df is not None:
        if task_annot_df.iloc[0, 0] >= pd.Timestamp("2020-11-01"):
            # Shift due to day-saving time
            task_annot_df.iloc[:, [0, 1, 2]] = task_annot_df.iloc[:,
                                                                  [0, 1, 2]] + pd.Timedelta(1, 'hour')
        writer = arus.mh.MhealthFileWriter(
            root, pid, hourly=True, date_folders=True)
        writer.set_for_annotation("HandHygieneTasks", "App")
        writer.write_csv(task_annot_df, append=False, block=True)

    return annot_df, task_annot_df


def _split_hand_clapping_annotations(start_time):
    start_time = pd.Timestamp(start_time).to_pydatetime()
    sts = []
    ets = []
    for offset_st, offset_et in HAND_CLAPPING_TIME_OFFSETS:
        st = start_time + offset_st
        et = start_time + offset_et
        sts.append(st)
        ets.append(et)
    return sts, ets


def _assemble_annotation_df(raw_annotations):
    label_names = raw_annotations['LABEL_NAME'].unique().tolist()
    dfs = []
    for label_name in label_names:
        start_times = []
        stop_times = []
        if '6 times' in label_name:
            old_start_times = raw_annotations.loc[(raw_annotations['LABEL_NAME'] == label_name) & (
                raw_annotations['EVENT_TYPE'] == "START"), 'HEADER_TIME_STAMP'].values
            # Only use the first hand clapping for syncing because the second hand clapping video has been changed between different sessions
            old_start_times = old_start_times[::2]
            for st in old_start_times:
                sts, ets = _split_hand_clapping_annotations(st)
                start_times = start_times + sts
                stop_times = stop_times + ets
        else:
            start_times = raw_annotations.loc[(raw_annotations['LABEL_NAME'] == label_name) & (
                raw_annotations['EVENT_TYPE'] == "START"), 'HEADER_TIME_STAMP'].values
            stop_times = raw_annotations.loc[(raw_annotations['LABEL_NAME'] == label_name) & (
                raw_annotations['EVENT_TYPE'] == "STOP"), 'HEADER_TIME_STAMP'].values
        if ":" in label_name:
            pruned_label_name = label_name.split(':')[1]
        else:
            pruned_label_name = label_name.split(' ')[1]
        label_df = pd.DataFrame(data={'HEADER_TIME_STAMP': start_times, 'START_TIME': start_times,
                                      'STOP_TIME': stop_times, 'LABEL_NAME': [pruned_label_name]*len(start_times)})
        dfs.append(label_df)
    if len(dfs) > 0:
        result_df = pd.concat(dfs, axis=0).sort_values(
            by=['HEADER_TIME_STAMP'])
    else:
        result_df = None
    return result_df


def _read_raw_annotation_file(filepath):
    raw_df = pd.read_csv(filepath, header=None,
                         infer_datetime_format=True, parse_dates=[0])
    raw_df.columns = ['HEADER_TIME_STAMP', 'LABEL_NAME', 'EVENT_TYPE']
    filter_condition = (raw_df['LABEL_NAME'].str.contains(
        'Collect ')) & (raw_df['LABEL_NAME'].str.contains(':'))
    task_filter_condition = (raw_df['LABEL_NAME'].str.contains(
        'Collect ')) & (~raw_df['LABEL_NAME'].str.contains(':'))
    raw_annotations = raw_df.loc[filter_condition, :]
    raw_task_annotations = raw_df.loc[task_filter_condition, :]
    logger.debug(raw_annotations)
    annot_df = _assemble_annotation_df(raw_annotations)
    task_annot_df = _assemble_annotation_df(raw_task_annotations)
    return annot_df, task_annot_df


if __name__ == "__main__":
    root = 'D:/datasets/hand_hygiene'
    pid = 'P3_4'
    sr = 80
    clean_up(root, pid, sr, auto_range='W-MON')
