"""
Writes metadata to an HDF5 file.
"""

import logging
import h5py
import numpy as np


logger = logging.getLogger(__name__)
_TSTATE_DATASET = '/TelescopeState'


def _array_encode(value):
    """Convert array of Unicode values to UTF-8 encoding for storage in HDF5"""
    if isinstance(value, bytes) or isinstance(value, unicode):
        # h5py has special handling for these: see h5py._hl.base.guess_dtype.
        return value
    value = np.asarray(value)
    if value.dtype.kind == 'U':
        return np.core.defchararray.encode(value, 'utf-8')
    else:
        return value


def set_telescope_model(h5_file, model_data, base_path="/TelescopeModel"):
    """Sets the tree of telescope model data on an HDF5 file."""
    for component in model_data.components.values():
        comp_base = "{0}/{1}/".format(base_path, component.name)
        try:
            c_group = h5_file.create_group(comp_base)
            c_group.attrs['class'] = str(component.__class__.__name__)
        except ValueError:
            c_group = h5_file[comp_base]
            logger.warning("Failed to create group %s (likely to already exist)", comp_base)
        for attribute in component.attributes:
            try:
                value = model_data.get_attribute_value(attribute)
                if value is not None:
                    c_group.attrs[attribute.name] = _array_encode(value)
            except Exception:
                logger.warning("Exception thrown while storing attribute %s",
                               attribute.name, exc_info=True)
        for sensor in sorted(component.sensors, key=lambda sensor: sensor.name):
            try:
                data = model_data.get_sensor_values(sensor)
                if data is not None:
                    try:
                        dset = np.rec.fromrecords(data, names='timestamp, value, status')
                        dset.sort(axis=0)
                        c_group.create_dataset(sensor.name, data=dset)
                        if sensor.description is not None:
                            c_group[sensor.name].attrs['description'] = sensor.description
                    except IndexError:
                        logger.warning("Failed to create dataset %s/%s as the model has no values",
                                       comp_base, sensor.name)
                    except RuntimeError:
                        logger.warning("Failed to insert dataset %s/%s as it already exists",
                                       comp_base, sensor.name)
            except Exception:
                logger.warning("Exception thrown while storing sensor %s",
                               sensor.name, exc_info=True)


def set_telescope_state(h5_file, tstate, base_path=_TSTATE_DATASET, start_timestamp=0):
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
        if not tstate.is_immutable(key):
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
