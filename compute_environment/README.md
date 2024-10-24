# Compute Environment

Instructions on how the compute environment can be setup (using Python 3.12.4).

## ESGF Environment

**Using Conda**:
The following has been tested on a CELS VM:
```bash
# Clone ESGF repo to access the environment file
git clone https://github.com/CROCUS-Urban/ingests.git
cd ingests/

# Create the virtual environment (tested with Python 3.12.4)
conda env create -f environment.yml
cd ../
rm -rf ingests/
conda activate ingests-dev

# Install Globus tools
pip install --upgrage pip
pip install globus-compute-endpoint

# Downgrade cryptography to avoid having the warning messages
pip install cryptography==42.0.0
```

**Using Miniconda**:
To create your miniconda3 environment, isolate the `.sh` file that fits the operating system hosting your web portal (find the complete list [here](https://repo.anaconda.com/miniconda/)).

Create the miniconda3 folder.
```bash
YOUR_SELECTED_FILE="<your-selected-file.sh>"
wget https://repo.anaconda.com/miniconda/$YOUR_SELECTED_FILE
chmod +x $YOUR_SELECTED_FILE
./$YOUR_SELECTED_FILE -b -p ./miniconda3/
rm $YOUR_SELECTED_FILE
```

Create your virtual environment with the same python version used with the Globus Compute endpoint (here 3.12.4)
```bash
./miniconda3/bin/conda create -y -n esgf-crocus python=3.12.4
```

Activate your environment, and install the necessary packages (tested on Polaris).
```bash
source ./miniconda3/bin/activate ./miniconda3/envs/esgf-crocus
pip install -r requirements.txt
```

**Using Pip**:
The requirements.txt was generated from Python 3.12.4 ESGF environment. Once you created and activated your python virtual environment, install the relevant packages.
```bash
pip install -r requirements.txt
```

## Globus Compute Function

Register the ESGF CROCUS ingest-wxt function.
```bash
python register_functions/gc_ingest_wxt.py
```

## Globus Compute Endpoint

Create an endpoint using the either the `esgf_crocus_local_config.yaml` or the `esgf_crocus_polaris_config.yaml` config file. Make sure to 1) customize the `worker_init` field to activate your environment and 2) add the function UUIDs in the `allowed_functions` field.
```bash
globus-compute-endpoint configure --endpoint-config endpoint_configs/YOUR-TARGER-CONFIE-FILE.yaml esgf_crocus
globus-compute-endpoint start esgf_crocus
```

## Test Framework

To test the Globus Compute framework, create an `.env` file with your Globus Compute endpoint and function UUIDs, and the full path (without `~/`) of your output directory (`odir`).
```bash
ENDPOINT_UUID="your-compute-endpoint-uuid"
FUNCTION_UUID="your-compute-function-uuid"
ODIR="your-full-path-output-directory"
```

Install dotenv to allow the testing function to access the environment variables in the `.env` file.
```bash
pip install python-dotenv
```

Run the test script.
```bash
python test_function.py
```

This should print the list of output files and an example of a generated dataset. The Globus Compute function itself returns the list of output files.
