# Requirements and Use Cases

Below are some of the key requirements for the workflows included in this project:

## Data Logging (Flow Use Case #1)
- Take data from the instrument and write it to a file
- Frequency: ~1 per hour
- Example: Vaisala WXT instrument which measures temperature, moisture, wind, precipitation, pressure
  - Serial --> USB device connects to computer
  - Python script uses Pyserial to interface, writing 10Hz data to a file
  - Run this in the background - need to check to make sure this is still running, resubmitting the script if neccessary

## Data Ingest (Flow Use Case #2)
- Once the raw data is collected, we need to clean the data (called "ingest")
  - Standard units
  - Aggregate into daily files
  - Fix times (where neccessary)
  - Add standard file naming + add helpful metadata
  - Transfer to Argonne National Lab (ANL) Archive
- Frequency - ~1 per day
- Example: Ceilometer data
  -  Need to transfer from some instrument computer to ANL
  -  Corections for the time in UTC need to be applied
  -  Calibration/dervived parameters need to be applied
  -  Daily files need ot created, placed in the ANL archive

This data can be made discoverable within the CROCUS project, via Globus shared collection

## Data Publication + Archiving (Flow Use Case #3)
- Long term archiving needs to be done with [ESS-Dive](https://ess-dive.lbl.gov/)
- We need to be able to:
  - Securely transfer data to ESS-Dive
  - Add in required metadata + point to existing data stores
  - Create quicklooks for our team to review before hitting "submit"
  - Automate this for several datastreams at a single push
- Example: 1 Month of Data from 5 sites
  - Say we have 5 sites with WXT sensors
  - We need to review the calibration, ensuring the data is consistent and there are not any major gaps (if so, highlight that)
  - Securely transfer hundreds of files, making sure files are not corrupted 
