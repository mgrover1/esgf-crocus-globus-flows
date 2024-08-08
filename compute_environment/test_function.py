from globus_compute_sdk import Client, Executor
import xarray as xr

# Define UUIDs
endpoint_uuid = "8af71930-d8ac-42cc-805c-48c7aa96db39"
function_uuid = "7c57d7aa-c9c8-4d83-90db-2352b1cfa6fb"

# Create Globus SDK Executor (currently using your own user credentials)
gcc = Client()
gce = Executor(endpoint_id=endpoint_uuid, client=gcc, amqp_port=443)

# Prepare payload for ESGF ingest-wxt
data = {
    "ndays": 1,
    "y": 2024,
    "m": 8,
    "d": 1,
    "site": 'NU',
    "hours": 1,
    "odir": "/home/bcote/CROCUS/output/"
}

# Start the task
future = gce.submit_to_registered_function(function_uuid, kwargs=data)

# Wait and print the result
result = future.result()
print(result)

# Preview one of the recently created file
if len(result) > 0:
    ds = xr.open_dataset(result[-1])
    print(ds)