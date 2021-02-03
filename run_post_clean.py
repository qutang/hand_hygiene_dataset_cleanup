
from loguru import logger
import os
import arus
import hhdataset
import argparse


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "pids", help="A list of pid sessions to run clean up, separated with comma. E.g., P1_1,P1_2 or ALL for all participants' sessions"
    )
    parser.add_argument(
        "--root", help="The root folder of the raw hand hygiene dataset", default="D:/Datasets/hand_hygiene_dataset"
    )
    return parser


if __name__ == "__main__":
    # configurations
    parser = setup_args()
    args = parser.parse_args()
    root = args.root
    pids = args.pids
    if pids == 'ALL':
        pids = arus.mh.get_pids(root)
        pids = list(filter(lambda pid: pid.startswith('P'), pids))
    else:
        pids = pids.split(',')
    for pid in pids:
        logger.info(f"Post clean {pid}")
        handle_id = logger.add(os.path.join(
            root, pid, "Logs", "post_clean.log"))
        hhdataset.convert_expert_annotations(root, pid)
        logger.remove(handle_id)
