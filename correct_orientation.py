import arus
import pandas as pd
from loguru import logger


def correct(sensor_df, annot_df, placement):
    stand_still_annot = annot_df.loc[annot_df.LABEL_NAME ==
                                     'Stand still', :].iloc[0, :]
    st = stand_still_annot['START_TIME']
    et = stand_still_annot['STOP_TIME']

    stand_still_df = arus.ext.pandas.segment_by_time(
        sensor_df, seg_st=st + pd.Timedelta(0.5, 's'), seg_et=et - pd.Timedelta(0.5, 's'))

    left_side = stand_still_df.iloc[:, 1].median() > 0
    if (placement == 'LW' and not left_side) or (placement == 'RW' and left_side):
        logger.info(
            f'{placement} sensor is upside down, correct it by flipping X and Y axes.')
        sensor_df.iloc[:, 1:4] = arus.ext.numpy.flip_and_swap(
            sensor_df.iloc[:, 1:4].values, '-x', '-y', 'z')
        if sensor_df.shape[1] > 4:
            logger.info('Flip IMU sensors...')
            sensor_df.iloc[:, 5:8] = arus.ext.numpy.flip_and_swap(
                sensor_df.iloc[:, 5:8].values, '-x', '-y', 'z')
            sensor_df.iloc[:, 8:11] = arus.ext.numpy.flip_and_swap(
                sensor_df.iloc[:, 8:11].values, '-x', '-y', 'z')
    return sensor_df
