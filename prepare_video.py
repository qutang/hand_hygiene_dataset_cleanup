import hhdataset as hhd
import argparse
import os


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "pids", help="A list of pid sessions to process videos, separated with comma. E.g., P1_1,P1_2"
    )
    parser.add_argument(
        "--root", help="The root folder of the raw hand hygiene dataset", default="D:/Datasets/hand_hygiene_dataset_openset"
    )

    parser.add_argument(
        "--cores", help="The number of cores used to process videos",
        default=1
    )
    return parser


if __name__ == "__main__":
    parser = setup_args()
    args = parser.parse_args()
    pids = args.pids.split(',')
    root = args.root
    cores = int(args.cores)
    for pid in pids:
        in_folder = os.path.join(root, pid, "OriginalRaw", "videos")
        video_converter = hhd.RawVideo(in_folder)
        video_converter.overlap_and_stitch()
