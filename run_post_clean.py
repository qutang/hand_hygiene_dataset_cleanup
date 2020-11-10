
import post_clean_up as post_clean
from loguru import logger
import os
import sys
import arus

if __name__ == "__main__":
    # configurations
    print(sys.argv)
    root = os.path.expanduser("D:/Datasets/hand_hygiene_dataset")
    if sys.argv[1] == 'ALL':
        pids = arus.mh.get_pids(root)
        pids = list(filter(lambda pid: pid.startswith('P'), pids))
    else:
        pids = sys.argv[1].split(',')

    print(pids)
    for pid in pids:
        logger.info(f"Post clean {pid}")
        handle_id = logger.add(os.path.join(
            root, pid, "Logs", "post_clean.log"))
        post_clean.convert_to_mhealth(root, pid)
        logger.remove(handle_id)
