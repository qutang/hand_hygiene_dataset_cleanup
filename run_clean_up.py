import arus
import hhdataset
from loguru import logger
import os
import argparse


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "pids", help="A list of pid sessions to run clean up, separated with comma. E.g., P1_1,P1_2"
    )
    parser.add_argument(
        "--root", help="The root folder of the raw hand hygiene dataset", default="D:/Datasets/hand_hygiene_dataset"
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
    parser = setup_args()
    args = parser.parse_args()
    # configurations
    pids = args.pids.split(',')
    root = args.root
    sr = args.sr
    date_range = None
    auto_range = "W-SAT"
    skip_convert_sensor = args.skipsensor
    skip_signaligner = args.skipsignaligner
    skip_sync = args.skipsync
    correct_ori = not args.skipcorrect

    profile_names = {
        'P6': ['Wonderwoman', 'Demo'],
        'P2': ['Black Widow', 'Demo'],
        'P3': ['Batman', 'Demo']
    }

    date_ranges = {
        'P6_1': [['2020-08-06', '2020-08-07'], ['2020-11-08-13', '2020-11-08-14']],
        'P6_2': [['2020-08-07', '2020-08-08'], ['2020-11-08-13', '2020-11-08-14']],
        'P6_3': [['2020-08-07', '2020-08-08'], ['2020-11-09-19', '2020-11-09-20']],
        'P6_4': [['2020-08-07', '2020-08-08'], ['2020-11-09-19', '2020-11-09-20']],
        'P6_5': [['2020-08-08', '2020-08-09'], ['2020-11-09-20', '2020-11-09-21']],
        'P6_6': [['2020-08-08', '2020-08-09'], ['2020-11-09-20', '2020-11-09-21']],
        'P2_1': [['2020-08-06', '2020-08-07'], ['2020-11-07-09', '2020-11-07-10']],
        'P2_2': [['2020-08-07', '2020-08-08'], ['2020-11-07-13', '2020-11-07-14']],
        'P2_3': [['2020-08-07', '2020-08-08'], ['2020-11-07-16', '2020-11-07-17']],
        'P2_4': [['2020-08-07', '2020-08-08'], ['2020-11-07-22', '2020-11-07-23']],
        'P2_5': [['2020-08-08', '2020-08-09'], ['2020-11-08-08', '2020-11-08-09']],
        'P2_6': [['2020-08-08', '2020-08-09'], ['2020-11-08-11', '2020-11-08-12']],
        'P3_1': [['2020-08-06', '2020-08-07'], ['2020-11-29-17', '2020-11-29-18']],
        'P3_2': [['2020-08-07', '2020-08-08'], ['2020-11-29-17', '2020-11-29-18']],
        'P3_3': [['2020-08-07', '2020-08-08'], ['2020-11-29-17', '2020-11-29-18']],
        'P3_4': [['2020-08-07', '2020-08-08'], ['2020-11-29-17', '2020-11-29-18']],
        'P3_5': [['2020-08-08', '2020-08-09'], ['2020-11-29-17', '2020-11-29-18']],
        'P3_6': [['2020-08-08', '2020-08-09'], ['2020-11-29-17', '2020-11-29-18']]
    }

    for pid in pids:
        handle_id = logger.add(os.path.join(root, pid, "Logs", "cleanup.log"))
        if pid.split('_')[0] in ['P2', 'P3', 'P6']:
            if not skip_convert_sensor:
                hhdataset.convert_to_mhealth(
                    root, pid, skip_sync, correct_orientation=correct_ori,
                    annot_profile=profile_names[pid.split('_')[0]][0],
                    raw_location='subj_folder',
                    remove_exists=True)
            else:
                logger.info(
                    'Skip converting sensors to mhealth')
            if not skip_signaligner:
                arus.cli.convert_to_signaligner_both(
                    root, pid, sr, date_range=date_ranges[pid][0], auto_range=auto_range)
            else:
                logger.info("Skip converting sensors to signaligner")

            if not skip_convert_sensor:
                hhdataset.convert_to_mhealth(
                    root, pid, skip_sync, correct_orientation=correct_ori,
                    annot_profile=profile_names[pid.split('_')[0]][1],
                    raw_location='cross_folder',
                    remove_exists=False)
            else:
                logger.info(
                    'Skip converting sensors to mhealth')

            if not skip_signaligner:
                arus.cli.convert_to_signaligner_both(
                    root, pid, sr, date_range=date_ranges[pid][1], auto_range=auto_range)
            else:
                logger.info("Skip converting sensors to signaligner")

        else:
            if not skip_convert_sensor:
                hhdataset.convert_to_mhealth(
                    root, pid, skip_sync, correct_orientation=correct_ori,
                    annot_profile='**',
                    raw_location='subj_folder',
                    remove_exists=True)
            else:
                logger.info(
                    'Skip converting sensors to mhealth')
            if not skip_signaligner:
                arus.cli.convert_to_signaligner_both(
                    root, pid, sr, date_range=None, auto_range=auto_range)
            else:
                logger.info("Skip converting sensors to signaligner")

        logger.remove(handle_id)
