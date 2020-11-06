# Script to clean up AGHH (App-guided hand hygiene) dataset

See the description about the dataset at: https://github.com/mhealthgroup/personal_hand_hygiene_detection.  
See the cleaned up dataset at: https://github.com/mhealthgroup/personal_hand_hygiene_detection.  
See the hosted unprocessed dataset at: https://github.com/qutang/hand_hygiene_dataset.  

## Usage

1. Clean up and convert raw data to mhealth format

    ```bash
    python run_clean_up.py [PID_LIST]
    ```

2. Copy expert-corrected annotations from signaligner back to AGHH

    ```bash
    python run_post_clean.py [PID_LIST]
    ```