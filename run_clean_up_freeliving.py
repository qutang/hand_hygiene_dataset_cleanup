import arus
import hhdataset
from loguru import logger
import os
import sys
import argparse


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "pids", help="A list of pid sessions to run clean up, separated with comma. E.g., P1_1,P1_2"
    )
    parser.add_argument(
        "--root", help="The root folder of the raw hand hygiene dataset", default="D:/Datasets/hand_hygiene_dataset_openset"
    )
    parser.add_argument(
        "--sr", help="The sampling rate of the sensors used in the dataset. Default is 80",
        default=80, type=int
    )
    parser.add_argument(
        "--skipconvertsensor", help="Skip converting sensors to mhealth when cleaning up the data",
        default=False, action='store_true'
    )
    parser.add_argument(
        "--skipsignaligner", help="Skip converting to signaligner when cleaning up the data",
        default=False, action='store_true'
    )
    parser.add_argument(
        "--skipsync", help="Skip synchronization when cleaning up the data",
        default=False, action='store_true'
    )
    parser.add_argument(
        "--skipcorrect", help="Skip correct orientation when cleaning up the data",
        default=False, action='store_true'
    )
    return parser


if __name__ == "__main__":
    # configurations
    parser = setup_args()
    args = parser.parse_args()
    pids = args.pids.split(',')
    root = args.root
    sr = args.sr
    date_range = None
    auto_range = "W-SUN"
    skip_sync = args.skipsync
    skip_convert_sensor = args.skipconvertsensor
    skip_signaligner = args.skipsignaligner
    correct_ori = not args.skipcorrect

    for pid in pids:
        handle_id = logger.add(os.path.join(root, pid, "Logs", "cleanup.log"))
        if not skip_convert_sensor:
            hhdataset.convert_to_mhealth(
                root, pid, skip_sync, correct_orientation=correct_ori,
                annot_profile='**',
                raw_location='subj_folder',
                remove_exists=False, skip_sensors=True)
        else:
            logger.info(
                'Skip converting sensors to mhealth')
        if not skip_signaligner:
            arus.cli.convert_to_signaligner_both(
                root, pid, sr, date_range=None, auto_range=auto_range)
        else:
            logger.info("Skip converting sensors to signaligner")
        logger.remove(handle_id)
