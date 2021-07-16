import glob
import os
import shutil

import arus
import pandas as pd
from loguru import logger


def convert_expert_annotations(root, pid):
    log_file = arus.mh.get_subject_log(root, pid, 'post_cleanup.log')
    if os.path.exists(log_file):
        os.remove(log_file)
    handle_id = logger.add(log_file)
    _convert_expert_annotations(root, pid)
    logger.remove(handle_id)


def _get_expert_annotation_file(pid):
    expert_folder = os.path.join(os.path.expanduser('~'), 'Documents',
                                 "SignalignerData", "export")
    names = os.listdir(expert_folder)
    expert_names = list(filter(lambda name: name.startswith(pid), names))
    if len(expert_names) == 1:
        return [os.path.join(expert_folder, expert_name) for expert_name in expert_names]
    elif len(expert_names) == 0:
        logger.warning(
            'Please make sure you use Signaligner to check the annotations before running post-clean command. Skip this one.')
        return None
    elif len(expert_names) > 1:
        logger.warning(
            "More than one expert annotation files are found!")
        return [os.path.join(expert_folder, expert_name) for expert_name in expert_names]


def _read_expert_annotation_file(filepath):
    raw_df = pd.read_csv(filepath, header=0,
                         infer_datetime_format=True, parse_dates=[0, 1])
    raw_df.insert(0, 'HEADER_TIME_STAMP', raw_df['START_TIME'])
    raw_df = raw_df.rename(columns={'PREDICTION': 'LABEL_NAME'})
    raw_df = raw_df.loc[raw_df['SOURCE'] == "Player", :]
    expert_annot_df = raw_df.loc[:, ['HEADER_TIME_STAMP',
                                     'START_TIME', 'STOP_TIME', 'LABEL_NAME']]
    expert_annot_df.loc[expert_annot_df['LABEL_NAME']
                        == "Unknown 1", 'LABEL_NAME'] = "Left"
    expert_annot_df.loc[expert_annot_df['LABEL_NAME']
                        == "Unknown 2", 'LABEL_NAME'] = "Right"
    expert_annot_df.loc[expert_annot_df['LABEL_NAME']
                        == "Unknown 3", 'LABEL_NAME'] = "Both"
    return expert_annot_df


def _convert_expert_annotations(root, pid):
    logger.info(
        "Convert expert annotations to mhealth format for hand hygiene raw dataset")

    logger.info('Update expert annotation file')
    src_expert_paths = _get_expert_annotation_file(pid)
    if src_expert_paths is not None:
        existing_expert_annots = glob.glob(os.path.join(
            root, pid,
            arus.mh.MASTER_FOLDER,
            '**', '*Expert*.annotation.csv'), recursive=True)
        for path in existing_expert_annots:
            logger.info(
                f'Remove existing expert annotation file {path}')
            os.remove(path)
        for src_expert_path in src_expert_paths:
            expert_annot_df = _read_expert_annotation_file(
                src_expert_path)

            if expert_annot_df is not None:
                writer = arus.mh.MhealthFileWriter(
                    root, pid, hourly=True, date_folders=True)
                writer.set_for_annotation("HandHygiene", "Expert")
                writer.write_csv(expert_annot_df, append=False, block=True)
            else:
                logger.warning(
                    "No expert annotated hand side information is found, skip this task.")


if __name__ == "__main__":
    convert_expert_annotations()('D:/datasets/hand_hygiene', 'P1-1')
