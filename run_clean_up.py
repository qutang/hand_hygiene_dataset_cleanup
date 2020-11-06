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
    auto_range = "W-WED"
    skip_sync = False
    correct_ori = True

    for pid in pids:
        handle_id = logger.add(os.path.join(root, pid, "Logs", "cleanup.log"))
        clean_up.convert_to_mhealth(
            root, pid, skip_sync, correct_orientation=correct_ori, remove_exists=True)
        arus.cli.convert_to_signaligner_both(
            root, pid, sr, date_range, auto_range)
        logger.remove(handle_id)
