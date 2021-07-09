import shutil
from glob import glob
import os
from loguru import logger
import argparse


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "root", help="The root folder of the raw hand hygiene dataset"
    )
    parser.add_argument(
        "output", help="The output folder of the cleaned hand hygiene dataset"
    )
    return parser


if __name__ == "__main__":
    parser = setup_args()
    args = parser.parse_args()
    root = args.root
    output_folder = args.output

    discard_annot_files = glob(os.path.join(
        output_folder, '*/MasterSynced/**/HandHygiene.Expert-HandHygiene.*.annotation.csv'), recursive=True)
    for f in discard_annot_files:
        logger.info(f'Remove {f}')
        os.remove(f)

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
