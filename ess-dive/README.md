# ESS-Dive

Instructions to use ESS-Dive to archive data

* [Setup](#ess-dive)
* [Get Authentication Token](#get-authentication-token)
* [Prepare metadata](#prepare-metadata)
* [Submit](#submit)

## Setup

```bash
conda create -n crocus-ess-dive-env python==3.11.9 --y
conda activate crocus-ess-dive-env
pip install requests
```
## Get Authentication Token

* Go to https://data-sandbox.ess-dive.lbl.gov
* Sign in with Orcid
* Click your Name in the right hand corner and select My Profile (Figure 1)
* Now Click the Settings>Authentication Token (Figure 2)
* Scroll down and click Copy on the “Token” tab to get your authentication token (Figure 2)

## Prepare metadata

Adapt the [metadata.json](./metadata.json) file to the metadata details you'd like to submit.
Also move the files and folders you would like to upload to within a single directory. The script will recursively traverse through that directory and upload datasets

## Submit

```bash
python3 archive-data.py --token <paste_here> --json_metadata metadata.json --upload_directory ./test-directory/
```
