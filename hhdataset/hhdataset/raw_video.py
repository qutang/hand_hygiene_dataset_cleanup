import os
import sys
from os import sep
import time
import subprocess
import cv2
import datetime
import tqdm
from multiprocessing import Pool
from loguru import logger
from moviepy.video.io.ffmpeg_tools import ffmpeg_extract_subclip
from .data import blank_video_path
import shutil


class RawVideo:
    def __init__(self, video_folder):
        self._folder = video_folder

    def overlap_and_stitch(self):
        self.stitch_videos()

    def overlap_timestamps(self, cores=1):
        video_file_list = os.listdir(self._folder)
        video_file_list = list(
            filter(lambda f: f.endswith('mp4'), video_file_list))

        logger.info('Video files found: ' + str(video_file_list))

        with Pool(processes=cores) as pp:
            for video_file in video_file_list:
                logger.info('processing: ' + str(video_file))
                stamped_video_file = os.path.join(self._folder, 'stamped', os.path.basename(
                    video_file).replace('.mp4', '_stamped.mp4'))
                logger.info(stamped_video_file)
                if os.path.exists(stamped_video_file):
                    logger.info(
                        f'{stamped_video_file} was already stamped, skip it')
                else:
                    p = pp.apply_async(RawVideo.add_timestamp_overlay, args=(
                        self._folder, video_file, None, False, True))
            pp.close()
            pp.join()

    def stitch_videos(self):
        file_list_txt = open(self._folder + sep + 'file_list.txt', 'w')
        final_output = os.path.join(self._folder, "..", 'stitched.mp4')
        try:
            os.remove(final_output)
        except OSError:
            pass

        start = time.time()
        for file in os.listdir(self._folder):
            if file.endswith('.mp4'):
                file_to_add = os.path.join(os.path.abspath(self._folder), file)

                logger.info(
                    "File to add: " + file_to_add)
                file_list_txt.write("file '%s'\n" % file_to_add)
        file_list_txt.close()
        video_files = self._folder + sep + 'file_list.txt'

        logger.info("Input text file: " + str(video_files))

        # merge the video files
        cmd = ["ffmpeg",
               "-f",
               "concat",
               "-safe",
               "0",
               "-loglevel",
               "warning",
               "-i",
               "%s" % video_files,
               "-c",
               "copy",
               "%s" % final_output
               ]

        logger.info("Command is: " + str(cmd))
        p = subprocess.run(cmd, capture_output=True, shell=True)

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd)

        end = time.time()
        logger.info(f"Merging videos took {end - start} seconds.")

        cmd = ["ffmpeg",
               "-i",
               final_output,
               "-vf",
               "scale=640:480",
               "-crf",
               "18",
               "-loglevel",
               "warning",
               "-preset",
               "veryfast",
               "-n",
               final_output.replace(".mp4", "_640x480.mp4")]

        p = subprocess.run(cmd, shell=True)

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd)

        # Remvoe the temp file.txt
        os.remove(video_files)

    @staticmethod
    def add_timestamp_overlay(in_path, file_name, out_path=None, preview=True, progress_bar=True):
        # keep the outpath same as the input path
        out_path = out_path or in_path

        # get the full path of the input file
        in_file_name = in_path + sep + file_name

        # Get the video file into openCV
        cap = cv2.VideoCapture(in_file_name)

        # Get the frame rate of the file
        video_frame_rate = cap.get(cv2.CAP_PROP_FPS)

        # Get the number of frames of the file
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Get frame size of the file
        frame_width, frame_height = int(cap.get(3)), int(cap.get(4))

        # File name length
        file_name_len = len(file_name)

        # remove extension
        video_start_time = file_name[:- 4]

        # Remove the begining QVR_ type string
        video_start_time = video_start_time[4:]

        # Split the file name by '_' to get date time components
        video_name_string_list = video_start_time.split("_")

        # Stitch a timestamp
        video_start_time = video_name_string_list[0] + "-" + video_name_string_list[1] + "-" + video_name_string_list[2] + \
            " " + \
            video_name_string_list[3] + ":" + video_name_string_list[4] + ":" + video_name_string_list[5] + \
            ".000"

        # Convert to a datetime object
        video_start_time = datetime.datetime.strptime(
            video_start_time, '%Y-%m-%d %H:%M:%S.%f')

        logger.info("Start time is: " + str(video_start_time))

        # This line below may not be working. Might be a codec problem
        fourcc = cv2.VideoWriter_fourcc(*'MP4V')

        # Create an output file name
        out_put_dir = out_path + sep + 'stamped'
        if not os.path.exists(out_put_dir):
            os.makedirs(out_put_dir)
        output_file = out_put_dir + sep + f'{file_name[:- 4]}_stamped.mp4'

        # Create a video writer buffer
        out = cv2.VideoWriter(output_file, fourcc,
                              video_frame_rate, (frame_height, frame_width))

        # Initialize the frame count
        frame_count = 0

        logger.info("Video frame rate is: " + str(video_frame_rate))

        with tqdm.tqdm(total=frame_count, disable=not progress_bar) as bar:

            while cap.isOpened():

                # Capture frames in the video
                ret, frame = cap.read()

                # rotate 90 degree counter-clockwise
                frame = cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)

                if ret:
                    bar.update()
                    # describe the type of font
                    # to be used.
                    font = cv2.FONT_HERSHEY_SIMPLEX

                    # Use putText() method for
                    # inserting text on video

                    # Get number of seconds to ad to the start time fetched from file name
                    secs_add = frame_count / video_frame_rate
                    secs_to_add = datetime.timedelta(seconds=secs_add)
                    frame_time = (video_start_time +
                                  secs_to_add).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    frame_count = frame_count + 1
                    frame = cv2.putText(frame,
                                        str(frame_time),
                                        (30, 30),
                                        font, 0.75,
                                        (0, 255, 255),
                                        2,
                                        cv2.LINE_4)
                    bar.set_description(frame_time)
                    # Display the resulting frame
                    # if end of the frame, exit
                    if not ret:
                        break

                    out.write(frame)

                    if preview:
                        cv2.imshow('video', frame)

                    # creating 'q' as the quit
                    # button for the video
                    if cv2.waitKey(1) & 0xFF == ord('q'):
                        break

                else:
                    break

        # release the cap object
        cap.release()
        out.release()
        # close all windows
        cv2.destroyAllWindows()

    def insert_blank(self):
        FIXED_VIDEO_LENGTH = 10 * 60

        video_files_list = os.listdir(self._folder)

        for i in range(len(video_files_list)):
            print(video_files_list[i])
            if i < len(video_files_list) - 1:
                file_1_init, file_1_time = RawVideo.get_time_from_video_filename(
                    video_files_list[i])
                file_2_init, file_2_time = RawVideo.get_time_from_video_filename(
                    video_files_list[i + 1])
                time_lag = file_2_time - \
                    (file_1_time + datetime.timedelta(seconds=FIXED_VIDEO_LENGTH))
                time_lag = time_lag.total_seconds()
                blank_start_time = file_1_time + \
                    datetime.timedelta(seconds=FIXED_VIDEO_LENGTH)
                blank_video_loc = self._folder + sep + \
                    RawVideo.generate_file_name(file_1_init, blank_start_time)
                RawVideo.trim_video(
                    blank_video_path, time_lag, blank_video_loc)

    @staticmethod
    def get_time_from_video_filename(file):
        # first remove extension
        filename = file[: -4]
        # split by components
        file_name_parts = filename.split('_')
        init_letters = file_name_parts[0]
        date_time_string = file_name_parts[1] + '-' + file_name_parts[2] + '-' + file_name_parts[3] + ' ' + \
            file_name_parts[4] + ':' + \
            file_name_parts[5] + ':' + file_name_parts[6]
        date_time_obj = datetime.datetime.strptime(
            date_time_string, '%Y-%m-%d %H:%M:%S')
        return init_letters, date_time_obj

    @staticmethod
    def generate_file_name(init_chars, date_time):
        date_time_string = date_time.strftime('%Y_%m_%d_%H_%M_%S')
        file_name = init_chars + '_' + date_time_string + '.mp4'
        return file_name

    @staticmethod
    def trim_video(video_file, trim_time, output_loc):
        ffmpeg_extract_subclip(
            video_file, 0, trim_time, targetname=output_loc)
