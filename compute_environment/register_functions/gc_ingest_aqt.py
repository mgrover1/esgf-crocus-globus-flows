"""
Ingests a number of days of CROCUS aqt data

Usage:
    python ./ingest-aqt.py ndays year month day site out_directory

Author:
    Scott Collis - 5.9.2024a

Note:
    Adapted for Globus Compute by Benoit Cote
"""

# Import Globus Compute SDK
import globus_compute_sdk

# Adaptation of the ingest-aqt.py CROCUS-Urban file
# https://github.com/CROCUS-Urban/ingests/blob/main/scripts/ingest-aqt.py
def gc_ingest_aqt(ndays=None, y=None, m=None, d=None, site=None, hours=24, odir="./"):
    """
        Arguments:
            ndays (int): Number of days to ingest
            y (int): Year start
            m (int): Month start
            d (int): Day start
            site (str): CROCUS Site
            hours (int): Number of hours in the time window
            odir (str): Full path (no ~/) output directory (must exist)

        Note:
            If y, m, d are all None, it will assume the current date (UTC)
    """

    # [EDITS from original] - Site definition moved here before site validation step
    aqt_global_NEIU = {'conventions': "CF 1.10",
                       'site_ID' : "NEIU",
                      'CAMS_tag' : "CMS-AQT-004",
                      'datastream' : "crocus_neiu_aqt_a1",
                      'datalevel' : "a1",
                       "plugin" : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                       'WSN' : 'W08D',
                      'latitude' : 41.9804526,
                      'longitude' : -87.7196038}
    
    aqt_global_NU = {'conventions': "CF 1.10",
                      'WSN':'W099',
                      'site_ID' : "NU",
                      'CAMS_tag' : "CMS-AQT-003",
                      'datastream' : "crocus_nu_aqt_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                      'datalevel' : "a1",
                      'latitude' : 42.051469749,
                      'longitude' : -87.677667183}
    
    aqt_global_CSU = {'conventions': "CF 1.10",
                      'WSN':'W08E',
                       'site_ID' : "CSU",
                      'CAMS_tag' : "CMS-AQT-002",
                      'datastream' : "crocus_csu_aqt_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                      'datalevel' : "a1",
                      'latitude' : 41.71991216,
                      'longitude' : -87.612834722}
    
    aqt_global_ATMOS = {'conventions': "CF 1.10",
                        'WSN':'W0A4',
                        'site_ID' : "ATMOS",
                        'CAMS_tag' : "CMS-AQT-001",
                        'datastream' : "crocus_atmos_aqt_a1",
                        'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                        'datalevel' : "a1",
                        'latitude' : 41.7016264,
                        'longitude' : -87.9956515}

    aqt_global_UIC = {'conventions': "CF 1.10",
                      'WSN':'W096',
                      'site_ID' : "UIC",
                      'CAMS_tag' : "CMS-AQT-",
                      'datastream' : "crocus_uic_aqt_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                      'datalevel' : "a1",
                      'latitude' : 41.869407936,
                      'longitude' : -87.645806251}

    aqt_global_CCI = {'conventions': "CF 1.10",
                      'WSN':'W08B',
                      'site_ID' : "NEIU_CCIS",
                      'CAMS_tag' : "CMS-AQT-",
                      'datastream' : "crocus_neiu_ccics_aqt_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                      'datalevel' : "a1",
                      'latitude' : 41.823038311,
                      'longitude' : -87.609379028}
    
    aqt_global_BIG = {'conventions': "CF 1.10",
                      'WSN':'W0A0',
                      'site_ID' : "BIG",
                      'CAMS_tag' : "CMS-AQT-14",
                      'datastream' : "crocus_big_aqt_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                      'datalevel' : "a1",
                      'latitude' : 41.77702369,
                      'longitude' : -87.609721059}

    aqt_global_HUM = {'conventions': "CF 1.10",
                      'WSN':'W0A1',
                      'site_ID' : "HUM",
                      'CAMS_tag' : "CMS-AQT-017",
                      'datastream' : "crocus_hum_aqt_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                      'datalevel' : "a1",
                      'latitude' : 41.905513206,
                      'longitude' : -87.703525713}
    
    aqt_global_DOWN = {'conventions': "CF 1.10",
                      'WSN':'W09D',
                      'site_ID' : "DOWN",
                      'CAMS_tag' : "CMS-AQT-010",
                      'datastream' : "crocus_down_aqt_a1",
                      'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                      'datalevel' : "a1",
                      'latitude' : 41.701476659,
                      'longitude' : -87.9953044}
    
    aqt_global_SHEDD = {'conventions': "CF 1.10",
                        'WSN':'W09E',
                        'site_ID' : "SHEDD",
                        'CAMS_tag' : "CMS-AQT-019",
                        'datastream' : "crocus_shedd_aqt_a1",
                        'plugin' : "registry.sagecontinuum.org/jrobrien/waggle-aqt:0.23.5.04",
                        'datalevel' : "a1",
                        'latitude' : 41.867918965,
                        'longitude' : -87.613535027}
    
    
    #put these in a dictionary for accessing

    global_sites = {'NU' : aqt_global_NU, 
                    'CSU': aqt_global_CSU,
                    'NEIU' : aqt_global_NEIU,
                    'ATMOS': aqt_global_ATMOS,
                    'UIC': aqt_global_UIC,
                    'NEIU_CCICS': aqt_global_CCI,
                    "BIG": aqt_global_BIG,
                    'HUM': aqt_global_HUM,
                    "DOWN": aqt_global_DOWN,
                    "SHEDD": aqt_global_SHEDD}

    if not site in global_sites:
        raise ValueError(f"'site' must be one of the following: {global_sites.keys()}.")

    # [EDITS from original] -- Extract current date if date not provided as inputs
    NoneType = type(None)
    if isinstance(y, NoneType) and isinstance(m, NoneType) and isinstance(d, NoneType):
        from datetime import datetime
        import pytz
        now = datetime.now(pytz.timezone("UTC"))
        y, m, d = now.year, now.month, now.day

    # [EDITS from original] -- Validate input date
    else:
        if (not isinstance(y, int)) or (not isinstance(m, int)) or (not isinstance(d, int)):
            raise ValueError("'y', 'm', and 'd' must be integers, or all None if selecting today (US/Central).")

    # [EDITS from original] -- Validate other selected inputs
    if not isinstance(ndays, int):
        raise ValueError("'ndays' must be an integer.")
    if not isinstance(hours, int):
        raise ValueError("'hours' must be an integer.")
    if not isinstance(odir, str):
        raise ValueError("'odir' must be a string.")

    # [EDITS from original] -- Check if directory exists
    import os
    if not os.path.isdir(odir):
        raise ValueError("'odir' directory does not exists. Make sure to use a full path (no ~/)")

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
    import argparse

    # [EDITS from original] -- Added hours as a variable
    def ingest_aqt(st, global_attrs, var_attrs, hours=24, odir=odir):
        """
            Ingest from CROCUS aqts using the Sage Data Client. 

            Ingests a whole day of aqt data and saves it as a NetCDF to odir
        
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
        df_aq = sage_data_client.query(start=start,
                                    end=end, 
                                            filter={
                                                "plugin" : global_attrs['plugin'],
                                                "vsn" : global_attrs['WSN'],
                                                "sensor" : "vaisala-aqt530"
                                            }
        )
        pm25 = df_aq[df_aq['name']=='aqt.particle.pm2.5']
        pm10 = df_aq[df_aq['name']=='aqt.particle.pm1']
        pm100 = df_aq[df_aq['name']=='aqt.particle.pm10']
        no = df_aq[df_aq['name']=='aqt.gas.no']
        o3 = df_aq[df_aq['name']=='aqt.gas.ozone']
        no2 = df_aq[df_aq['name']=='aqt.gas.no2']
        co = df_aq[df_aq['name']=='aqt.gas.co']
        aqtemp = df_aq[df_aq['name']=='aqt.env.temp']
        aqhum = df_aq[df_aq['name']=='aqt.env.humidity']
        aqpres = df_aq[df_aq['name']=='aqt.env.pressure']

        # Convert instrument timestamp to Pandas Datatime object
        pm25['time'] = pd.DatetimeIndex(pm25['timestamp'].values)

        # Remove all meta data descriptions besides the index
        aqvals = pm25.loc[:, pm25.columns.intersection(["time"])]

        # Add all parameter to the output dataframe
        aqvals['pm2.5'] = pm25.value.to_numpy().astype(float)
        aqvals['pm1.0'] = pm10.value.to_numpy().astype(float)
        aqvals['pm10.0'] = pm100.value.to_numpy().astype(float)

        aqvals['no'] = no.value.to_numpy().astype(float)
        aqvals['o3'] = o3.value.to_numpy().astype(float)
        aqvals['no2'] = no2.value.to_numpy().astype(float)
        aqvals['co'] = co.value.to_numpy().astype(float)
        aqvals['temperature'] =  aqtemp.value.to_numpy().astype(float)
        aqvals['humidity'] =  aqhum.value.to_numpy().astype(float)
        aqvals['pressure'] =  aqpres.value.to_numpy().astype(float)

        # calculate dewpoint from relative humidity
        dp = dewpoint_from_relative_humidity(aqvals.temperature.to_numpy() * units.degC, 
                                            aqvals.humidity.to_numpy() * units.percent
        )
        aqvals['dewpoint'] = dp
        
        # Define the index
        aqvals = aqvals.set_index("time")
        
        end_fname = st.strftime('-%Y%m%d-%H%M%S.nc')
        start_fname = odir + '/crocus-' + global_attrs['site_ID'] + '-' + 'aqt-'+ global_attrs['datalevel']
        fname = start_fname + end_fname
        valsxr = xr.Dataset.from_dataframe(aqvals)
        valsxr = valsxr.sortby('time')
        
        # Assign the global attributes
        valsxr = valsxr.assign_attrs(global_attrs)
        # Assign the individual parameter attributes
        for varname in var_attrs.keys():
            valsxr[varname] = valsxr[varname].assign_attrs(var_attrs[varname])
        # Check if file exists and remove if necessary
        try:
            os.remove(fname)
        except OSError:
            pass
        
        # ---------
        # Apply QC
        #----------
        # Check for aerosol water vapor uptake and mask out
        cond = (0 < valsxr.humidity) & (valsxr.humidity < 98)
        valsxr = valsxr.where(cond, drop=False) 
        
        # Ensure time is saved properly
        valsxr["time"] = pd.to_datetime(valsxr.time)

        if valsxr['pm2.5'].shape[0] > 0:
            valsxr.to_netcdf(fname, format='NETCDF4')
        else:
            print('not saving... no data')
        
        return fname

    # Variable definitions
    var_attrs_aqt = {'pm2.5' : {'standard_name' : 'mole_concentration_of_pm2p5_ambient_aerosol_particles_in_air',
                                'units' : 'ug/m^3'},
                    'pm10.0' : {'standard_name' : 'mole_concentration_of_pm10p0_ambient_aerosol_particles_in_air',
                                'units' : 'ug/m^3'},
                    'pm1.0' : {'standard_name' : 'mole_concentration_of_pm1p0_ambient_aerosol_particles_in_air',
                               'units' : 'ug/m^3'},
                    'no' : {'standard_name' : 'mole_fraction_of_nitrogen_monoxide_in_air',
                            'units' : 'Parts Per Million'},
                    'o3' : {'standard_name' : 'mole_fraction_of_ozone_in_air',
                            'units' : 'Parts Per Million'},
                    'co' : {'standard_name' : 'mole_fraction_of_carbon_monoxide_in_air',
                            'units' : 'Parts Per Million'},
                    'no2' : {'standard_name' : 'mole_fraction_of_nitrogen_dioxide_in_air',
                            'units' : 'Parts Per Million'},
                    'temperature': {'standard_name' : 'air_temperature',
                            'units' : 'celsius'},
                    'humidity': {'standard_name' : 'relative_humidity',
                            'units' : 'percent'},
                    'dewpoint': {'standard_name' : 'dew_point_temperature',
                            'units' : 'celsius'},
                    'pressure': {'standard_name' : 'air_pressure',
                            'units' : 'hPa'}}
    
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
            fname = ingest_aqt(this_date,  site_args, var_attrs_aqt, odir=args.odir)
            print("Succeed")
            fname_list.append(fname) # [EDITS from original]
        except Exception as e:
            print("Fail", e)

    # [EDITS from original] -- return Globus Compute result
    return fname_list


# Creating Globus Compute client
gcc = globus_compute_sdk.Client()

# Register the function
COMPUTE_FUNCTION_ID = gcc.register_function(gc_ingest_aqt)

# Write function UUID in a file
uuid_file_name = "gc_ingest_aqt_uuid.txt"
with open(uuid_file_name, "w") as file:
    file.write(COMPUTE_FUNCTION_ID)
    file.write("\n")
file.close()

# End of script
print("Function registered with UUID -", COMPUTE_FUNCTION_ID)
print("The UUID is stored in " + uuid_file_name + ".")
print("")