import clean_up
import arus
from loguru import logger
import os
import sys

if __name__ == "__main__":
    # configurations
    pids = sys.argv[1].split(',')
    root = os.path.expanduser("D:/Datasets/hand_hygiene_dataset")
    sr = 80
    date_range = None
    auto_range = "W-TUE"
    skip_sync = False
    correct_ori = True

    profile_names = {
        'P6': ['Wonderwoman', 'Demo'],
        'P2': ['Black Widow', 'Demo']
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
        'P2_6': [['2020-08-08', '2020-08-09'], ['2020-11-08-11', '2020-11-08-12']]
    }

    for pid in pids:
        handle_id = logger.add(os.path.join(root, pid, "Logs", "cleanup.log"))
        if pid.split('_')[0] in ['P2', 'P6']:
            # clean_up.convert_to_mhealth(
            #     root, pid, skip_sync, correct_orientation=correct_ori,
            #     annot_profile=profile_names[pid.split('_')[0]][0],
            #     raw_location='subj_folder',
            #     remove_exists=True)
            # arus.cli.convert_to_signaligner_both(
            #     root, pid, sr, date_range=date_ranges[pid][0], auto_range=auto_range)
            clean_up.convert_to_mhealth(
                root, pid, skip_sync, correct_orientation=correct_ori,
                annot_profile=profile_names[pid.split('_')[0]][1],
                raw_location='cross_folder',
                remove_exists=False)
            arus.cli.convert_to_signaligner_both(
                root, pid, sr, date_range=date_ranges[pid][1], auto_range=auto_range)
        else:
            clean_up.convert_to_mhealth(
                root, pid, skip_sync, correct_orientation=correct_ori,
                annot_profile='**',
                raw_location='subj_folder',
                remove_exists=True)
            arus.cli.convert_to_signaligner_both(
                root, pid, sr, date_range=None, auto_range=auto_range)
        logger.remove(handle_id)
