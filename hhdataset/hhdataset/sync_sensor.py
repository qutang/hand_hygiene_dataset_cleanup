

import datetime as dt
import glob
import os

import arus
import numpy as np
import pandas as pd
from loguru import logger
from scipy import signal


def _get_sync_periods(annot_df):
    condition = annot_df.iloc[:, 3] == 'SYNC_COLLECT'
    sync_annotations = annot_df.loc[condition, :].iloc[:, [1, 2]]
    sync_annotations['START_TIME'] = sync_annotations['START_TIME'] + \
        pd.Timedelta(np.timedelta64(-1, 's'))
    sync_annotations['STOP_TIME'] = sync_annotations['START_TIME'] + \
        pd.Timedelta(np.timedelta64(19, 's'))
    return sync_annotations


def _combine_peaks(vm_peak_indices, z_peak_indices):
    loop_indices = z_peak_indices if len(z_peak_indices) < len(
        vm_peak_indices) else vm_peak_indices
    compare_indices = vm_peak_indices if len(z_peak_indices) < len(
        vm_peak_indices) else z_peak_indices
    keep_indices = []
    for i in loop_indices:
        if np.any(np.abs(np.array(compare_indices) - i) <= 5):
            keep_indices.append(i)
    return keep_indices


def _detect_claps(sync_sensor_df, sync_annot_df, height, x_height, y_height, z_height, distance, use_axs):
    ts = sync_sensor_df.iloc[:, 0]
    vm_values = arus.ext.numpy.vector_magnitude(
        sync_sensor_df.iloc[:, 1:4])[:, 0]
    x_values = sync_sensor_df.iloc[:, 1]
    y_values = sync_sensor_df.iloc[:, 2]
    z_values = sync_sensor_df.iloc[:, 3]
    height = 3 if height == -1 else height
    x_height = 1 if x_height == -1 else x_height
    y_height = 1 if y_height == -1 else y_height
    z_height = 1 if z_height == -1 else z_height
    distance = 20 if distance == -1 else distance

    vm_peak_indices = signal.find_peaks(
        vm_values, height, distance=distance)[0].tolist()
    x_peak_indices = signal.find_peaks(
        x_values, x_height, distance=distance)[0].tolist()
    y_peak_indices = signal.find_peaks(
        y_values, y_height, distance=distance)[0].tolist()
    z_peak_indices = signal.find_peaks(
        z_values, z_height, distance=distance)[0].tolist()

    if use_axs == 'all':
        peak_indices = _combine_peaks(
            vm_peak_indices, x_peak_indices)
        peak_indices = _combine_peaks(
            peak_indices, y_peak_indices)
        peak_indices = _combine_peaks(
            peak_indices, z_peak_indices)
    elif use_axs == 'x':
        peak_indices = x_peak_indices
    elif use_axs == 'vm':
        peak_indices = vm_peak_indices
    elif use_axs == 'y':
        peak_indices = y_peak_indices
    elif use_axs == 'z':
        peak_indices = z_peak_indices

    logger.info(f'{len(peak_indices)} clap peaks found')
    while len(peak_indices) != 6:
        logger.warning(
            "The number of peaks seem not correct, the peak finder setting may need to be adjusted!!!")
        logger.warning(
            f'Current peak finding settings: height={height}, z_height={z_height}, distance={distance}')
        height = input("Try a new peak height (g). Default is None.\n:")
        height = float(height) if height != "" else None
        x_height = input("Try a new peak x height (g). Default is None.\n:")
        x_height = float(x_height) if x_height != "" else None
        y_height = input("Try a new peak y height (g). Default is None.\n:")
        y_height = float(y_height) if y_height != "" else None
        z_height = input("Try a new peak z height (g). Default is None.\n:")
        z_height = float(z_height) if z_height != "" else None
        distance = input(
            "Try a new inter-peak distance (# of samples). Default is None.\n:")
        distance = int(distance) if distance != "" else None
        use_axs = input(
            "Choose which axs to use. Default is all.\n:")
        use_axs = use_axs if use_axs != "" else 'all'

        vm_peak_indices = signal.find_peaks(
            vm_values, height, distance=distance)[0].tolist()
        x_peak_indices = signal.find_peaks(
            x_values, x_height, distance=distance)[0].tolist()
        y_peak_indices = signal.find_peaks(
            y_values, y_height, distance=distance)[0].tolist()
        z_peak_indices = signal.find_peaks(
            z_values, z_height, distance=distance)[0].tolist()

        if use_axs == 'all':
            peak_indices = _combine_peaks(
                vm_peak_indices, x_peak_indices)
            peak_indices = _combine_peaks(
                peak_indices, y_peak_indices)
            peak_indices = _combine_peaks(
                peak_indices, z_peak_indices)
        elif use_axs == 'x':
            peak_indices = x_peak_indices
        elif use_axs == 'vm':
            peak_indices = vm_peak_indices
        elif use_axs == 'y':
            peak_indices = y_peak_indices
        elif use_axs == 'z':
            peak_indices = z_peak_indices

        logger.info(f'{len(peak_indices)} clap peaks found')

    peak_indices = sorted(peak_indices)
    logger.debug(peak_indices)

    peak_ts = ts.iloc[peak_indices]
    st = peak_ts - pd.Timedelta(np.timedelta64(50, 'ms'))
    et = peak_ts + pd.Timedelta(np.timedelta64(50, 'ms'))
    clap_df = pd.DataFrame(data={
        'HEADER_TIME_STAMP': st.tolist(),
        'START_TIME': st.tolist(),
        'STOP_TIME': et.tolist(),
        'LABEL_NAME': "Sync peaks"
    }, index=range(len(peak_ts)))
    return clap_df, height, x_height, y_height, z_height, distance, use_axs


def _get_average_sensor_offset(annot_df, peak_annot_df):
    # Only use the last three peaks for the first hand clapping activities during sync task
    peaks_from_sensor = peak_annot_df.iloc[[2, 3, 4], :]
    peaks_from_annot = annot_df.loc[annot_df['LABEL_NAME'].str.contains(
        '6 times'), :].iloc[[2, 3, 4], :]

    peaks_ts_from_sensor = peaks_from_sensor['START_TIME'] + (
        peaks_from_sensor['STOP_TIME'] - peaks_from_sensor['START_TIME']) / 2.0
    peaks_ts_from_annot = peaks_from_annot['START_TIME']
    peaks_ts_from_sensor.reset_index(drop=True, inplace=True)
    peaks_ts_from_annot.reset_index(drop=True, inplace=True)
    average_offset = (peaks_ts_from_annot - peaks_ts_from_sensor).mean()
    logger.info(f'Timestamp offset for this sensor is: {average_offset}')
    return average_offset


def _sync_sensor_to_annotations(sensor_df, average_offset):
    sensor_df['HEADER_TIME_STAMP'] = sensor_df['HEADER_TIME_STAMP'] + \
        average_offset
    return sensor_df


def _sync(sensor_df, annot_df, task_annot_df, height=-1, x_height=-1, y_height=-1, z_height=-1, use_axs='all', distance=20, init_offset=None, pid=None, sid=None, data_type=None):
    if init_offset is None:
        init_offset = input(
            "Set the init offset for sensor data, default is 0:")
        init_offset = 0 if len(init_offset) == 0 else float(init_offset)
    sync_annots = _get_sync_periods(task_annot_df)
    average_offsets = []
    logger.info(
        f'Found {sync_annots.shape[0]} synchronization markers. Analyzing them...')
    for row in sync_annots.itertuples(index=False):
        st = row.START_TIME
        et = row.STOP_TIME
        print(st)
        print(et)
        sync_sensor_df = arus.ext.pandas.segment_by_time(
            sensor_df, seg_st=st + pd.Timedelta(init_offset, 's'), seg_et=et + pd.Timedelta(init_offset, 's'))

        print(sync_sensor_df.head(n=3))
        print(sync_sensor_df.tail(n=3))

        if pid == 'P9_5' and sid == 'TAS1E23150167':
            sync_sensor_df = arus.ext.pandas.segment_by_time(
                sensor_df, seg_st=pd.Timestamp('2020-08-12 18:17:33.700'), seg_et=pd.Timestamp('2020-08-12 18:17:38.300'))
        elif pid == 'P9_5' and sid == 'TAS1E23150866':
            sync_sensor_df = arus.ext.pandas.segment_by_time(
                sensor_df, seg_st=pd.Timestamp('2020-08-12 18:17:34.200'), seg_et=pd.Timestamp('2020-08-12 18:17:38.800'))

        sync_annot_df = arus.ext.pandas.segment_by_time(
            annot_df, seg_st=st, seg_et=et, st_col=1, et_col=2)
        if sync_sensor_df.empty:
            logger.warning(
                "Did not find corresponding sensor data for the current synchronization markers.")
            peak_kwargs = {
                'height': -1,
                'x_height': -1,
                'y_height': -1,
                'z_height': -1,
                'distance': -1,
                'init_offset': init_offset,
                'use_axs': 'all'
            }
        else:
            sync_peak_df, height, x_height, y_height, z_height, distance, use_axs = _detect_claps(
                sync_sensor_df, sync_annot_df, height, x_height, y_height, z_height, distance, use_axs)
            average_offset = _get_average_sensor_offset(
                sync_annot_df, sync_peak_df)
            average_offsets.append(average_offset)
            peak_kwargs = {
                'height': height,
                'x_height': x_height,
                'y_height': y_height,
                'z_height': z_height,
                'distance': distance,
                'init_offset': init_offset,
                'use_axs': use_axs
            }
    average_offset = np.mean(average_offsets)

    if pid == 'P9_5' and sid == 'TAS1E23150866' and 'IMU' in data_type:
        average_offset = pd.Timedelta(1.5, unit='s')
    elif pid == 'P9_6' and sid == 'TAS1E23150167' and data_type == 'AccelerometerCalibrated':
        average_offset = pd.Timedelta(2.13, unit='s')
    elif pid == 'P9_6' and sid == 'TAS1E23150866' and data_type == 'AccelerometerCalibrated':
        average_offset = pd.Timedelta(1.49, unit='s')
    elif pid == 'P9_6' and sid == 'TAS1E23150167' and 'IMU' in data_type:
        average_offset = pd.Timedelta(2.05, unit='s')

    logger.info(f'The sensor offsets are: {average_offsets}')
    logger.info(f'The average sensor offset is: {average_offset}')
    sensor_df = _sync_sensor_to_annotations(
        sensor_df, average_offset)
    return sensor_df, peak_kwargs
