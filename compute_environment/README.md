# Compute Environment

Instructions on how the compute environment was setup.

## ESGF Environment

Using Conda:
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

Using Pip (requirements.txt generated from Python 3.12.4 ESGF environment). Once you created and activated your python virtual environment, install the relevant packages.
```bash
pip install -r requirements.txt
```

## Globus Compute Endpoint

Create an endpoint using LocalProvider config file. Make sure to customize the `worker_init` field accordingly to activate your environment.
```bash
globus-compute-endpoint configure --endpoint-config endpoint_configs/esgf_crocus_config.yaml esgf_crocus
globus-compute-endpoint start esgf_crocus
```

## Globus Compute Function

Register the ESGF CROCUS ingest-wxt function.
```bash
python register_functions/gc_ingest_wxt.py
```

## Test Framework

To test the Globus Compute framework, customize the `test_function.py` file with your own UUIDs and your own output path (`odir`), and run the script.
```bash
python test_function.py
```
This should print the list of output files and an example of a generated dataset. The Globus Compute function itself returns the list of output files.
