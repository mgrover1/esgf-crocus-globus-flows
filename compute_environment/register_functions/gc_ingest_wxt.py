"""
Ingests a number of days of CROCUS WXT data

Usage:
    python ./ingest-wxt.py ndays year month day site out_directory

Author:
    Scott Collis - 5.9.2024a

Note:
    Adapted for Globus Compute by Benoit Cote
"""

# Import Globus Compute SDK
import globus_compute_sdk

# Adaptation of the ingest-wxt.py CROCUS-Urban file
# https://github.com/CROCUS-Urban/ingests/blob/main/scripts/ingest-wxt.py
def gc_ingest_wxt(ndays=None, y=None, m=None, d=None, site=None, hours=24, odir="./"):
    """
        Arguments:
            ndays (int): Number of days to ingest
            y (int): Year start
            m (int): Month start
            d (int): Day start
            site (str): CROCUS Site
            hours (int): Number of hours in the time window
            odir (str): Output directory (must exist)
    """

    # [EDITS from original] - Site definition moved here before site validation step
    wxt_global_NEIU = {'conventions': "CF 1.10",
                       'site_ID' : "NEIU",
                      'CAMS_tag' : "CMS-WXT-002",
                      'datastream' : "CMS_wxt536_NEIU_a1",
                      'datalevel' : "a1",
                       "plugin" : "registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.*",
                       'WSN' : 'W08D',
                      'latitude' : 41.9804526,
                      'longitude' : -87.7196038}
    wxt_global_NU = {'conventions': "CF 1.10",
                      'WSN':'W099',
                       'site_ID' : "NU",
                      'CAMS_tag' : "CMS-WXT-005",
                      'datastream' : "CMS_wxt536_NU_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.*",
                      'datalevel' : "a1",
                      'latitude' : 42.051469749,
                      'longitude' : -87.677667183}
    wxt_global_CSU = {'conventions': "CF 1.10",
                      'WSN':'W08E',
                       'site_ID' : "CSU",
                      'CAMS_tag' : "CMS-WXT-003",
                      'datastream' : "CMS_wxt536_CSU_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.*",
                      'datalevel' : "a1",
                      'latitude' : 41.71991216,
                      'longitude' : -87.612834722}

    # [EDITS from original] -- Make sure the selected CROCUS site is available
    global_sites = {
        'NU' : wxt_global_NU, 
        'CSU': wxt_global_CSU,
        'NEIU' : wxt_global_NEIU
    }
    if not site in global_sites:
        raise ValueError(f"'site' must be one of the following: {global_sites.keys()}.")

    # [EDITS from original] -- Validate other selected inputs
    if not isinstance(ndays, int):
        raise ValueError("'ndays' must be an integer.")
    if not isinstance(y, int):
        raise ValueError("'y' must be an integer.")
    if not isinstance(m, int):
        raise ValueError("'m' must be an integer.")
    if not isinstance(d, int):
        raise ValueError("'d' must be an integer.")
    if not isinstance(hours, int):
        raise ValueError("'hours' must be an integer.")
    if not isinstance(odir, str):
        raise ValueError("'odir' must be a string.")

    # Import packages
    import sage_data_client
    import matplotlib.pyplot as plt
    import pandas as pd
    from metpy.calc import dewpoint_from_relative_humidity, wet_bulb_temperature
    from metpy.units import units
    from PIL import Image
    import numpy as np
    import datetime
    import xarray as xr
    import os
    import argparse

    # [EDITS from original] -- Added hours as a variable
    def ingest_wxt(st, global_attrs, var_attrs, hours=hours, odir=odir):
        """
            Ingest from CROCUS WXTs using the Sage Data Client. 

            Ingests a whole day of WXT data and saves it as a NetCDF to odir
        
            Parameters
            ----------
            st : Datetime 
                Date to ingest

            global_attrs : dict
                Attributes that are specific to the site.
            
            var_attrs : dict
                Attributes that map variables in Beehive to
                CF complaint netCDF valiables.
        
            Returns
            -------
                [EDITS from original] name of output file
        
        """

        start = st.strftime('%Y-%m-%dT%H:%M:%SZ')
        end = (st + datetime.timedelta(hours=hours)).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(start)
        print(end)
        df_temp = sage_data_client.query(
            start=start,
            end=end, 
            filter={
                "name" : 'wxt.env.temp|wxt.env.humidity|wxt.env.pressure|wxt.rain.accumulation',
                "plugin" : global_attrs['plugin'],
                "vsn" : global_attrs['WSN'],
                "sensor" : "vaisala-wxt536"
            }
        )
        winds = sage_data_client.query(
            start=start,
            end=end, 
            filter={
                "name" : 'wxt.wind.speed|wxt.wind.direction',
                "plugin" : global_attrs['plugin'],
                "vsn" : global_attrs['WSN'],
                "sensor" : "vaisala-wxt536"
            }
        )
        
        hums = df_temp[df_temp['name']=='wxt.env.humidity']
        temps = df_temp[df_temp['name']=='wxt.env.temp']
        pres = df_temp[df_temp['name']=='wxt.env.pressure']
        rain = df_temp[df_temp['name']=='wxt.rain.accumulation']

        npres = len(pres)
        nhum = len(hums)
        ntemps = len(temps)
        nrains = len(rain)
        print(npres, nhum, ntemps, nrains)
        minsamps = min([nhum, ntemps, npres, nrains])

        temps['time'] = pd.DatetimeIndex(temps['timestamp'].values)

        vals = temps.set_index('time')[0:minsamps]
        vals['temperature'] = vals.value.to_numpy()[0:minsamps]
        vals['humidity'] = hums.value.to_numpy()[0:minsamps]
        vals['pressure'] = pres.value.to_numpy()[0:minsamps]
        vals['rainfall'] = rain.value.to_numpy()[0:minsamps]

        direction = winds[winds['name']=='wxt.wind.direction']
        speed = winds[winds['name']=='wxt.wind.speed']

        nspeed = len(speed)
        ndir = len(direction)
        print(nspeed, ndir)
        minsamps = min([nspeed, ndir])

        speed['time'] = pd.DatetimeIndex(speed['timestamp'].values)
        windy = speed.set_index('time')[0:minsamps]
        windy['speed'] = windy.value.to_numpy()[0:minsamps]
        windy['direction'] = direction.value.to_numpy()[0:minsamps]

        winds10mean = windy.resample('10s').mean(numeric_only=True).ffill()
        winds10max = windy.resample('10s').max(numeric_only=True).ffill()
        dp = dewpoint_from_relative_humidity(
            vals.temperature.to_numpy() * units.degC, 
            vals.humidity.to_numpy() * units.percent
        )

        vals['dewpoint'] = dp
        vals10 = vals.resample('10s').mean(numeric_only=True).ffill() #ffil gets rid of nans due to empty resample periods
        wb = wet_bulb_temperature(
            vals10.pressure.to_numpy() * units.hPa,
            vals10.temperature.to_numpy() * units.degC,
            vals10.dewpoint.to_numpy() * units.degC
        )

        vals10['wetbulb'] = wb
        vals10['wind_dir_10s'] = winds10mean['direction']
        vals10['wind_mean_10s'] = winds10mean['speed']
        vals10['wind_max_10s'] = winds10max['speed']
        _ = vals10.pop('value')
        
        end_fname = st.strftime('_%Y%m%d_%H%M%S.nc')
        start_fname = odir + '/crocus-' + global_attrs['site_ID'] + '-' + 'wxt-'+ global_attrs['datalevel']
        fname = start_fname + end_fname
        
        try:
            os.remove(fname)
        except OSError:
            pass
        
        vals10xr = xr.Dataset.from_dataframe(vals10)
        vals10xr = vals10xr.sortby('time')
        
        vals10xr = vals10xr.assign_attrs(global_attrs)
        
        for varname in var_attrs.keys():
            vals10xr[varname] = vals10xr[varname].assign_attrs(var_attrs[varname])
        
        vals10xr.to_netcdf(fname)
        
        # [EDITS from original] -- return output file path+name for Globus Compute function result
        return fname

    # Variable definitions
    var_attrs_wxt = {'temperature': {'standard_name' : 'air_temperature',
                           'units' : 'celsius'},
                    'humidity': {'standard_name' : 'relative_humidity',
                           'units' : 'percent'},
                    'dewpoint': {'standard_name' : 'dew_point_temperature',
                           'units' : 'celsius'},
                    'pressure': {'standard_name' : 'air_pressure',
                           'units' : 'hPa'},
                    'wind_mean_10s': {'standard_name' : 'wind_speed',
                           'units' : 'celsius'},
                    'wind_max_10s': {'standard_name' : 'wind_speed',
                           'units' : 'celsius'},
                    'wind_dir_10s': {'standard_name' : 'wind_from_direction',
                           'units' : 'degrees'},
                    'rainfall': {'standard_name' : 'precipitation_amount',
                           'units' : 'kg m-2'}}
    
    # [EDITS from original] -- Parsing the command line -- replaced by the following
    from dataclasses import dataclass
    @dataclass
    class ArgsClass:
        ndays: int
        y: int
        m: int
        d: int
        site: str
        odir: str
    args = ArgsClass(ndays, y, m, d, site, odir)

    print(args.ndays)
    start_date = datetime.datetime(args.y,args.m,args.d)
    site_args = global_sites[args.site]

    # [EDITS from original] -- declare list of output files to be returned as the Globus Compute result
    fname_list = []

    for i in range(args.ndays):
        this_date = start_date + datetime.timedelta(days=i)
        print(this_date)
        try:
            fname = ingest_wxt(this_date,  site_args, var_attrs_wxt, odir=args.odir)
            print("Succeed")
            fname_list.append(fname) # [EDITS from original]
        except Exception as e:
            print("Fail", e)

    # [EDITS from original] -- return Globus Compute result
    return fname_list


# Creating Globus Compute client
gcc = globus_compute_sdk.Client()

# Register the function
COMPUTE_FUNCTION_ID = gcc.register_function(gc_ingest_wxt)

# Write function UUID in a file
uuid_file_name = "gc_ingest_wxt_uuid.txt"
with open(uuid_file_name, "w") as file:
    file.write(COMPUTE_FUNCTION_ID)
    file.write("\n")
file.close()

# End of script
print("Function registered with UUID -", COMPUTE_FUNCTION_ID)
print("The UUID is stored in " + uuid_file_name + ".")
print("")
