"""
Writes metadata to an HDF5 file.
"""

import logging

import h5py
import numpy as np
import katsdptelstate


logger = logging.getLogger(__name__)
_TSTATE_DATASET = '/TelescopeState'


def set_telescope_state(h5_file: h5py.File,
                        tstate: katsdptelstate.TelescopeState,
                        base_path: str = _TSTATE_DATASET,
                        start_timestamp: float = 0.0) -> None:
    """Write raw pickled telescope state to an HDF5 file."""
    tstate_group = h5_file.create_group(base_path)
    # include the subarray product id for use by the crawler to identify which
    # system the file belongs to.
    tstate_group.attrs['subarray_product_id'] = tstate.get('subarray_product_id', 'none')
    tstate_keys = tstate.keys()
    logger.info("Writing {} telescope state keys to {}".format(len(tstate_keys), base_path))

    sensor_dtype = np.dtype(
        [('timestamp', np.float64),
         ('value', h5py.special_dtype(vlen=np.uint8))])
    for key in tstate_keys:
        if tstate.key_type(key) == katsdptelstate.KeyType.MUTABLE:
            # retrieve all values for a particular key
            sensor_values = tstate.get_range(key, st=start_timestamp,
                                             include_previous=True, return_encoded=True)
            # swap value, timestamp to timestamp, value
            sensor_values = [(timestamp, np.frombuffer(value, dtype=np.uint8))
                             for (value, timestamp) in sensor_values]
            dset = np.rec.fromrecords(sensor_values, dtype=sensor_dtype, names='timestamp,value')
            tstate_group.create_dataset(key, data=dset)
            logger.debug("TelescopeState: Written {} values for key {} to file".format(
                len(dset), key))
        else:
            tstate_group.attrs[key] = np.void(tstate.get(key, return_encoded=True))
            logger.debug("TelescopeState: Key {} written as an attribute".format(key))
