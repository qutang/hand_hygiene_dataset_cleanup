import shutil
from glob import glob
import os
from loguru import logger
import sys

if __name__ == "__main__":
    dataset = sys.argv[1]
    if dataset == 'aghh':
        root = 'D:/Datasets/hand_hygiene_dataset'
        output_folder = 'C:/Users/tqshe/projects/hand_hygiene_release/inhome_data'
    elif dataset == 'flhh':
        root = 'D:/Datasets/hand_hygiene_openset'
        output_folder = 'C:/Users/tqshe/projects/hand_hygiene_release/freeliving_data'
    else:
        raise NotImplementedError('This input argument is not supported.')

    master_files = glob(
        os.path.join(
            root, '*/MasterSynced/**/*.csv'), recursive=True)
    meta_files = glob(
        os.path.join(
            root, '*/Meta/*.csv'), recursive=True)

    for f in master_files + meta_files:
        logger.info(f'Copy {f}')
        output_path = f.replace(root, output_folder)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        shutil.copyfile(f, output_path)

    discard_annot_files = glob(os.path.join(
        output_folder, '*/MasterSynced/**/HandHygieneTasks.App-HandHygieneTasks*.annotation.csv'), recursive=True)
    for f in discard_annot_files:
        logger.info(f'Remove {f}')
        os.remove(f)

    discard_sensor_files = glob(os.path.join(
        output_folder, '*/MasterSynced/**/ActigraphGT9X-AccelerometerCalibrated*.sensor.csv'), recursive=True)
    for f in discard_sensor_files:
        logger.info(f'Remove {f}')
        os.remove(f)

    discard_sensor_files = glob(os.path.join(
        output_folder, '*/MasterSynced/**/ActigraphGT9X-IMUTemperature*.sensor.csv'), recursive=True)
    for f in discard_sensor_files:
        logger.info(f'Remove {f}')
        os.remove(f)
