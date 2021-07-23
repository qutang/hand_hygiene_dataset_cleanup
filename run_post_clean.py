
from loguru import logger
import os
import arus
import hhdataset
import argparse


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "pids", help="A list of pid sessions to run clean up, separated with comma. E.g., P1_1,P1_2 or P1,P2 or ALL for all participants' sessions"
    )
    parser.add_argument(
        "--root", help="The root folder of the raw hand hygiene dataset", default="D:/Datasets/hand_hygiene_dataset"
    )
    parser.add_argument(
        "--dry-run", help="Print out the selected PID sessions",
        default=False, action='store_true'
    )
    return parser


if __name__ == "__main__":
    # configurations
    parser = setup_args()
    args = parser.parse_args()
    root = args.root
    pids = args.pids
    all_pids = arus.mh.get_pids(root)
    all_pids = list(filter(lambda pid: pid.startswith('P'), all_pids))
    if pids == 'ALL':
        pids = all_pids
    else:
        pids = pids.split(',')
    pids = list(
        filter(lambda pid: pid in pids or pid.split("_")[0] in pids, all_pids))
    logger.info(f"Selected pids: {pids}")
    if args.dry_run:
        exit(0)
    for pid in pids:
        logger.info(f"Post clean {pid}")
        handle_id = logger.add(os.path.join(
            root, pid, "Logs", "post_clean.log"))
        hhdataset.convert_expert_annotations(root, pid)
        logger.remove(handle_id)
