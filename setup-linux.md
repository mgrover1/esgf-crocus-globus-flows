# Setup Linux Box

## 1. Install Globus Connect Personal
Follow the directions here https://docs.globus.org/globus-connect-personal/install/linux, including the section on [running as systemd](https://docs.globus.org/globus-connect-personal/install/linux/#running_globus_connect_personal_as_a_systemd_user_unit)
- Name the linux box as `atmos-linux-box-n` where n is the number of the box (ex. `atmos-linux-box-03`)
- Check on globus.org to make sure the endpoint is accessible

## 2. Install Python
Install miniforge https://github.com/conda-forge/miniforge for your system, which will give you an open-source mambaforge installation that is lightweight!
