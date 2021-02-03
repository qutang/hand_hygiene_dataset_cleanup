# Script to clean up AGHH (App-guided hand hygiene) dataset

See the description about the dataset at: https://github.com/mhealthgroup/personal_hand_hygiene_detection.  
See the cleaned up dataset at: https://github.com/mhealthgroup/personal_hand_hygiene_detection.  
See the hosted unprocessed dataset at: https://github.com/qutang/hand_hygiene_dataset.  

## Usage

1. Clean up and convert raw data to mhealth format

    ```bash
    # See help information
    python run_clean_up.py -h
    ```

2. Copy expert-corrected annotations from signaligner back to AGHH

    ```bash
    # See help information
    python run_post_clean.py -h
    ```

3. Send cleaned up AGHH to the public repo

    ```bash
    # See help information
    python send_clean.py -h
    ```
