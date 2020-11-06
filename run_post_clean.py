
import post_clean_up as post_clean
from loguru import logger
import os
import sys

if __name__ == "__main__":
    # configurations
    pids = sys.argv[1].split(',')
    root = os.path.expanduser("D:/Datasets/hand_hygiene_dataset")

    for pid in pids:
        handle_id = logger.add(os.path.join(
            root, pid, "Logs", "post_clean.log"))
        post_clean.convert_to_mhealth(root, pid)
        logger.remove(handle_id)
