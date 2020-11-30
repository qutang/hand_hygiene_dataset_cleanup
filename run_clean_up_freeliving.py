import clean_up
import arus
from loguru import logger
import os
import sys

if __name__ == "__main__":
    # configurations
    pids = sys.argv[1].split(',')
    root = os.path.expanduser("D:/Datasets/hand_hygiene_dataset_openset")
    sr = 80
    date_range = None
    auto_range = "W-SUN"
    skip_sync = True
    correct_ori = False

    for pid in pids:
        handle_id = logger.add(os.path.join(root, pid, "Logs", "cleanup.log"))
        clean_up.convert_to_mhealth(
            root, pid, skip_sync, correct_orientation=correct_ori,
            annot_profile='**',
            raw_location='subj_folder',
            remove_exists=False, skip_sensors=True)
        # arus.cli.convert_to_signaligner_both(
        #     root, pid, sr, date_range=None, auto_range=auto_range)
        logger.remove(handle_id)
